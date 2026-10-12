import sys
import time
import logging
import datetime
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import *

# ──────────────────────────────────────────────
# Logger setup — stdout so logs appear in the
# output stream, not the error stream
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# BOOKMARK VERSION — jobtype behaviour
# ──────────────────────────────────────────────
# historical (default, first run):
#   - Bypasses the bookmark entirely.
#   - Reads all raw partitions via spark.read.parquet() month-by-month.
#   - Writes with overwrite mode.
#   - Use this once to backfill all historical data.
#
# current (all subsequent scheduled runs):
#   - Uses glueContext.create_dynamic_frame with transformation_ctx so
#     Glue bookmark tracks which S3 files have already been processed.
#   - Only new files (added since the last successful job.commit()) are read.
#   - Writes with append mode.
#   - Requires Job bookmark = Enable in the Glue job settings.
# ──────────────────────────────────────────────

# ──────────────────────────────────────────────
# Initialize Glue job
# ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Resolve jobtype — defaults to 'historical' if not provided
try:
    _jobtype_args = getResolvedOptions(sys.argv, ['jobtype'])
    JOB_TYPE = _jobtype_args['jobtype'].lower()
except Exception:
    JOB_TYPE = 'historical'
    logger.info("No 'jobtype' parameter provided – defaulting to 'historical'")

logger.info("Job type: %s", JOB_TYPE)

sc = SparkContext()
sc.setLogLevel("WARN")   # suppress Spark/Py4J internal noise from error log
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ──────────────────────────────────────────────
# Spark tuning
# shuffle.partitions: 200 is too high for incremental (one month) runs.
# Use 8 for current mode (small data), 200 for historical full backfill.
# maxPartitionBytes: 256 MB reduces the number of tasks vs 128 MB default,
# lowering task-scheduling overhead without hurting throughput.
# ──────────────────────────────────────────────
if JOB_TYPE == 'historical':
    spark.conf.set("spark.sql.shuffle.partitions", "200")
else:
    spark.conf.set("spark.sql.shuffle.partitions", "8")

spark.conf.set("spark.sql.files.maxPartitionBytes",       "268435456")  # 256 MB
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.parquet.filterPushdown",        "true")
spark.conf.set("spark.sql.parquet.mergeSchema",           "false")

# ── Adaptive Query Execution (AQE) ────────────────────────────────────────────
# Automatically coalesces post-shuffle partitions based on actual data size,
# collapsing near-empty shuffle tasks at runtime.
# With shuffle.partitions=200 and only one month of incremental data,
# AQE merges 200 tasks down to the true data-driven count automatically.
spark.conf.set("spark.sql.adaptive.enabled",                            "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",         "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                   "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",       "268435456")  # 256 MB
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
account_id    = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET     = f"nyc-tlc-{account_id}"
RAW_BASE_PATH = f"s3://{S3_BUCKET}/raw/hvfhv"
CURATED_PATH  = f"s3://{S3_BUCKET}/curated/hvfhv/"

VALID_LICENSES = ['HV0002', 'HV0003', 'HV0004', 'HV0005']

logger.info("S3 bucket    : %s", S3_BUCKET)
logger.info("Raw path     : %s", RAW_BASE_PATH)
logger.info("Curated path : %s", CURATED_PATH)


# ──────────────────────────────────────────────
# List available year/month partitions in raw/
# (used by historical mode only)
# ──────────────────────────────────────────────
def list_raw_partitions(s3_client, bucket: str, prefix: str) -> list:
    """Return a sorted list of (year, month) tuples from hive-style partitions
    under raw/hvfhv/year=YYYY/month=MM/.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix + "/", Delimiter="/")

    partitions = []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            year_str = folder.replace("year=", "")
            if not year_str.isdigit():
                continue
            year = int(year_str)

            month_pages = paginator.paginate(
                Bucket=bucket,
                Prefix=cp["Prefix"],
                Delimiter="/"
            )
            for mp in month_pages:
                for mcp in mp.get("CommonPrefixes", []):
                    mfolder = mcp["Prefix"].rstrip("/").split("/")[-1]
                    month_str = mfolder.replace("month=", "")
                    if month_str.isdigit():
                        partitions.append((year, int(month_str)))

    return sorted(partitions)


# ──────────────────────────────────────────────
# Core transformation  (shared by both modes)
#
# Optimisations vs previous version:
#  - processing_timestamp: computed once as a Python literal (lit) instead of
#    current_timestamp() per-row. current_timestamp() is non-deterministic,
#    which blocks Spark's constant-folding and AQE optimisations.
#  - Monetary columns use double instead of decimal(10,2). Parquet stores
#    DECIMAL as fixed-length byte arrays (more bytes, slower IO). DOUBLE is
#    a native Parquet/Spark type — smaller on disk, faster to read/write,
#    and sufficient precision for fare values.
#  - Flag columns kept as boolean (compact bit storage in Parquet).
# ──────────────────────────────────────────────
def transform_raw_to_curated(raw_df, processing_ts: str):
    """Transform raw HVFHV data to curated layer."""

    ts_lit = lit(processing_ts)   # single literal, not per-row current_timestamp()

    curated_df = raw_df.select(
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),

        col("request_datetime").cast("timestamp").alias("request_datetime"),
        col("on_scene_datetime").cast("timestamp").alias("on_scene_datetime"),
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),

        col("PULocationID").cast("integer").alias("PULocationID"),
        col("DOLocationID").cast("integer").alias("DOLocationID"),

        coalesce(col("trip_miles"),           lit(0.0)).cast("double").alias("trip_miles"),
        col("trip_time").cast("integer").alias("trip_time_seconds"),
        coalesce(col("base_passenger_fare"),  lit(0.0)).cast("double").alias("base_passenger_fare"),
        coalesce(col("tolls"),                lit(0.0)).cast("double").alias("tolls"),
        coalesce(col("bcf"),                  lit(0.0)).cast("double").alias("bcf"),
        coalesce(col("sales_tax"),            lit(0.0)).cast("double").alias("sales_tax"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("double").alias("congestion_surcharge"),
        coalesce(col("airport_fee"),          lit(0.0)).cast("double").alias("airport_fee"),
        coalesce(col("tips"),                 lit(0.0)).cast("double").alias("tips"),
        coalesce(col("driver_pay"),           lit(0.0)).cast("double").alias("driver_pay"),

        (col("shared_request_flag") == "Y").alias("shared_request_flag"),
        (col("shared_match_flag")   == "Y").alias("shared_match_flag"),
        (col("access_a_ride_flag")  == "Y").alias("access_a_ride_flag"),
        (col("wav_request_flag")    == "Y").alias("wav_request_flag"),
        (col("wav_match_flag")      == "Y").alias("wav_match_flag"),

        lit(0.0).cast("double").alias("cbd_congestion_fee"),

        col("pickup_datetime").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),

        date_format("pickup_datetime", "EEEE").alias("day_of_week"),

        dayofweek("pickup_datetime").isin([1, 7]).alias("is_weekend"),

        (col("trip_time") / 60.0).cast("double").alias("trip_duration_minutes"),

        # total_fare — excludes tips
        (coalesce(col("base_passenger_fare"),  lit(0.0)) +
         coalesce(col("tolls"),                lit(0.0)) +
         coalesce(col("bcf"),                  lit(0.0)) +
         coalesce(col("sales_tax"),            lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"),          lit(0.0))
        ).cast("double").alias("total_fare"),

        # total_amount — includes tips
        (coalesce(col("base_passenger_fare"),  lit(0.0)) +
         coalesce(col("tolls"),                lit(0.0)) +
         coalesce(col("bcf"),                  lit(0.0)) +
         coalesce(col("sales_tax"),            lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"),          lit(0.0)) +
         coalesce(col("tips"),                 lit(0.0))
        ).cast("double").alias("total_amount"),

        when(col("trip_time") > 0,
             col("trip_miles") / (col("trip_time") / 3600.0)
        ).otherwise(None).cast("double").alias("avg_speed_mph"),

        (coalesce(col("airport_fee"), lit(0.0)) > 0).alias("is_airport_trip"),

        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"),

        ts_lit.alias("processing_timestamp")
    )

    return curated_df


# ──────────────────────────────────────────────
# Data quality validation  (shared by both modes)
# ──────────────────────────────────────────────
def validate_curated_data(curated_df):
    return curated_df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("PULocationID").isNotNull() &
        col("DOLocationID").isNotNull() &
        (col("trip_miles") >= 0) & (col("trip_miles") <= 500) &
        (col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 10000) &
        (col("dropoff_datetime") > col("pickup_datetime")) &
        (col("pickup_datetime") >= col("request_datetime")) &
        col("hvfhs_license_num").isin(VALID_LICENSES)
    )


# ──────────────────────────────────────────────
# Combined stats: total + rejection breakdown +
# license distribution — all in ONE Spark job.
#
# Replaces:
#   count()                 → 1 Spark job
#   log_rejection_breakdown → 1 Spark job (agg with 9 cols)
#   log_license_distribution→ 1 Spark job (groupBy)
# With:
#   single agg() pass       → 1 Spark job total
#
# License distribution is embedded as 4 conditional sums so no
# groupBy shuffle is needed at all.
# ──────────────────────────────────────────────
def collect_stats(curated_df, batch_label: str) -> dict:
    """Run one agg() over curated_df and return a stats dict."""
    row = curated_df.agg(
        # total rows
        count("*").alias("total"),

        # rejection rules
        _sum(when(col("pickup_datetime").isNull(),  1).otherwise(0)).alias("null_pickup"),
        _sum(when(col("dropoff_datetime").isNull(), 1).otherwise(0)).alias("null_dropoff"),
        _sum(when(col("PULocationID").isNull(),     1).otherwise(0)).alias("null_PU"),
        _sum(when(col("DOLocationID").isNull(),     1).otherwise(0)).alias("null_DO"),
        _sum(when(~((col("trip_miles") >= 0) & (col("trip_miles") <= 500)),                     1).otherwise(0)).alias("miles_oob"),
        _sum(when(~((col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 10000)), 1).otherwise(0)).alias("fare_oob"),
        _sum(when(~(col("dropoff_datetime") > col("pickup_datetime")),                          1).otherwise(0)).alias("dropoff_lte_pickup"),
        _sum(when(~(col("pickup_datetime") >= col("request_datetime")),                         1).otherwise(0)).alias("pickup_before_req"),
        _sum(when(~col("hvfhs_license_num").isin(VALID_LICENSES),                               1).otherwise(0)).alias("invalid_license"),

        # license distribution — 4 conditional sums, no groupBy shuffle
        _sum(when(col("hvfhs_license_num") == "HV0002", 1).otherwise(0)).alias("lic_HV0002"),
        _sum(when(col("hvfhs_license_num") == "HV0003", 1).otherwise(0)).alias("lic_HV0003"),
        _sum(when(col("hvfhs_license_num") == "HV0004", 1).otherwise(0)).alias("lic_HV0004"),
        _sum(when(col("hvfhs_license_num") == "HV0005", 1).otherwise(0)).alias("lic_HV0005"),
    ).collect()[0]

    return row.asDict()


def log_stats(stats: dict, batch_label: str):
    total = stats["total"] or 0

    rejection_rules = [
        ("null pickup_datetime",    stats["null_pickup"]),
        ("null dropoff_datetime",   stats["null_dropoff"]),
        ("null PULocationID",       stats["null_PU"]),
        ("null DOLocationID",       stats["null_DO"]),
        ("trip_miles out of range", stats["miles_oob"]),
        ("fare out of range",       stats["fare_oob"]),
        ("dropoff <= pickup",       stats["dropoff_lte_pickup"]),
        ("pickup before request",   stats["pickup_before_req"]),
        ("invalid license",         stats["invalid_license"]),
    ]

    lines = [f"Stats — {batch_label}  (total rows: {total:,})"]
    lines.append(f"\n  Rejection breakdown:")
    lines.append(f"  {'Rule':<30} {'Rejected':>10}  {'%':>7}")
    lines.append("  " + "-" * 52)
    for rule_name, n in rejection_rules:
        pct = (n / total * 100) if total else 0.0
        lines.append(f"  {rule_name:<30} {n:>10,}  {pct:>6.2f}%")

    lines.append(f"\n  License distribution:")
    lines.append(f"  {'License':<20} {'Count':>12}")
    lines.append("  " + "-" * 34)
    for lic in VALID_LICENSES:
        n = stats.get(f"lic_{lic}", 0)
        lines.append(f"  {lic:<20} {n:>12,}")

    logger.info("\n".join(lines))


# ──────────────────────────────────────────────
# Shared: transform → validate → write one batch
#
# Optimisations vs previous version:
#  - count() + agg() merged into a single collect_stats() call (1 Spark job).
#  - validated_df is NOT cached before the write — caching here forces Spark
#    to materialise the full dataset into memory/disk then re-read it for the
#    write. Removing the cache lets Spark stream straight through to S3.
#  - partitionBy("year", "month") only — dropping "day" eliminates ~30×
#    the number of output files per partition, avoiding the small-files
#    problem that slows both writes and downstream Athena queries.
# ──────────────────────────────────────────────
def process_and_write(raw_df, batch_label: str, write_mode: str):
    """Transform, validate and write a single batch. Returns (written, rejected) counts."""

    processing_ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Transform — lazy, no data read yet
    curated_df = transform_raw_to_curated(raw_df, processing_ts)

    # Cache curated_df — materialised by collect_stats() below (one S3 read).
    # Everything downstream reuses this cached plan.
    curated_df.cache()

    # Single Spark job: total count + all rejection stats + license distribution
    stats = collect_stats(curated_df, batch_label)
    total = stats["total"]

    logger.info("Rows loaded  : %s", f"{total:,}")

    if total == 0:
        logger.warning("Empty batch %s — skipping.", batch_label)
        curated_df.unpersist()
        return 0, 0

    log_stats(stats, batch_label)

    # Filter on cached curated_df — no S3 re-read
    validated_df = validate_curated_data(curated_df)

    write_start = time.time()
    logger.info("Writing to %s  (mode=%s) ...", CURATED_PATH, write_mode)

    # Write directly — no cache on validated_df.
    # Spark streams from the cached curated_df through the filter to S3.
    # partitionBy year+month only: avoids the small-files explosion that
    # year+month+day causes (~30× more files, slower writes & Athena scans).
    validated_df.write \
        .mode(write_mode) \
        .partitionBy("year", "month") \
        .parquet(CURATED_PATH)

    write_secs = time.time() - write_start
    logger.info("Write completed in %.1f seconds.", write_secs)

    curated_df.unpersist()

    # validated_df is a filter on the (now-unpersisted) curated_df plan.
    # One final count() gives us the true accepted row count.
    # This is the only remaining Spark action after the write — it resolves
    # quickly because the filter plan is simple and the data is already on S3.
    validated_count = validated_df.count()
    rejected_count  = total - validated_count
    rejection_rate  = (rejected_count / total * 100) if total else 0.0

    logger.info("Valid   : %s", f"{validated_count:,}")
    logger.info("Rejected: %s  (%.2f%%)", f"{rejected_count:,}", rejection_rate)

    return validated_count, rejected_count


# ═══════════════════════════════════════════════
# HISTORICAL MODE — month-by-month full backfill
# bypasses bookmark; writes with overwrite
# ═══════════════════════════════════════════════
job_start = time.time()
total_written = total_rejected = batches_ok = batches_skipped = 0

if JOB_TYPE == 'historical':
    logger.info("=" * 60)
    logger.info("HISTORICAL MODE — full backfill via partition loop")
    logger.info("Bookmark is NOT used. All raw partitions will be processed.")
    logger.info("=" * 60)

    s3_client  = boto3.client("s3")
    partitions = list_raw_partitions(s3_client, S3_BUCKET, "raw/hvfhv")

    if not partitions:
        logger.warning("No partitions found. Processing %s/ as a single batch.", RAW_BASE_PATH)
        partitions = [(None, None)]

    logger.info("Discovered %d partition(s) to process.", len(partitions))

    for (year_val, month_val) in partitions:
        if year_val is None:
            batch_path  = RAW_BASE_PATH + "/"
            batch_label = "all"
        else:
            batch_path  = f"{RAW_BASE_PATH}/year={year_val}/month={month_val:02d}/"
            batch_label = f"{year_val}-{month_val:02d}"

        logger.info("=" * 60)
        logger.info("Processing batch: %s  ->  %s", batch_label, batch_path)

        try:
            raw_df = spark.read.parquet(batch_path)
        except Exception as e:
            logger.warning("Skipping batch %s: %s", batch_label, e)
            batches_skipped += 1
            continue

        written, rejected = process_and_write(raw_df, batch_label, write_mode="overwrite")
        if written == 0 and rejected == 0:
            batches_skipped += 1
        else:
            total_written  += written
            total_rejected += rejected
            batches_ok     += 1
            logger.info("Batch %s done. Running total written: %s", batch_label, f"{total_written:,}")

# ═══════════════════════════════════════════════
# CURRENT MODE — bookmark-based incremental read
# uses DynamicFrame + transformation_ctx; writes with append
# ═══════════════════════════════════════════════
else:
    logger.info("=" * 60)
    logger.info("CURRENT MODE — incremental via Glue job bookmark")
    logger.info("Only files added since last job.commit() will be read.")
    logger.info("Requires Job bookmark = Enable in Glue job settings.")
    logger.info("=" * 60)

    # Register any new raw partitions that landed since the last run.
    # The crawler is only needed once (historical / first run) to create
    # the Glue Catalog table definition; MSCK REPAIR handles new partitions.
    logger.info("Running MSCK REPAIR TABLE on nyc_tlc_db.hvfhv ...")
    try:
        spark.sql("MSCK REPAIR TABLE nyc_tlc_db.hvfhv")
        logger.info("MSCK REPAIR TABLE nyc_tlc_db.hvfhv complete.")
    except Exception as e:
        logger.warning("MSCK REPAIR TABLE failed (continuing): %s", e)

    # Use DynamicFrame.count() to short-circuit before converting to DataFrame.
    # DynamicFrame.count() is a metadata-only operation on the Catalog —
    # it does not read S3 data. This avoids the raw_df.cache() + count() +
    # unpersist() dance from the previous version.
    raw_dyf = glueContext.create_dynamic_frame.from_catalog(
        database="nyc_tlc_db",
        table_name="hvfhv",
        transformation_ctx="raw_hvfhv_source",   # ← bookmark key
    )

    dyf_count = raw_dyf.count()
    logger.info("New records detected by bookmark: %s", f"{dyf_count:,}")

    if dyf_count == 0:
        logger.info("No new data since last run — bookmark is up to date.")
    else:
        # Convert to DataFrame only if there is data to process
        raw_df = raw_dyf.toDF()

        written, rejected = process_and_write(raw_df, "bookmark_run", write_mode="append")
        total_written  += written
        total_rejected += rejected
        batches_ok      = 1

# ──────────────────────────────────────────────
# Job-level summary
# ──────────────────────────────────────────────
job_secs = time.time() - job_start
job_mins = job_secs / 60

logger.info("=" * 60)
logger.info("JOB SUMMARY  (jobtype=%s)", JOB_TYPE)
logger.info("  Batches processed        : %d", batches_ok)
logger.info("  Batches skipped/failed   : %d", batches_skipped)
logger.info("  Total records written    : %s", f"{total_written:,}")
logger.info("  Total records rejected   : %s", f"{total_rejected:,}")
logger.info("  Total job duration       : %.1f seconds (%.1f minutes)", job_secs, job_mins)
logger.info("=" * 60)

# ──────────────────────────────────────────────
# MSCK REPAIR — sync Glue catalog with new
# curated partitions so Athena can query them
# without running a full crawler.
# ──────────────────────────────────────────────
logger.info("Running MSCK REPAIR TABLE on nyc_tlc_db.curated_hvfhv ...")
spark.sql("MSCK REPAIR TABLE nyc_tlc_db.curated_hvfhv")
logger.info("MSCK REPAIR TABLE complete.")

# Commit — advances the bookmark for current mode;
# required to finalise the job for historical mode.
logger.info("Committing job.")
job.commit()
