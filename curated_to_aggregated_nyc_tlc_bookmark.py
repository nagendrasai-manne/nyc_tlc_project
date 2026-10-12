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
from pyspark.sql.window import Window

# ── Logging setup — stdout so logs appear in output stream, not error stream ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("curated_to_aggregated_bookmark")

# ──────────────────────────────────────────────────────────────────────────────
# BOOKMARK VERSION — jobtype behaviour
# ──────────────────────────────────────────────────────────────────────────────
# historical (default, first run):
#   - Bypasses the bookmark entirely.
#   - Reads all curated partitions via spark.read.parquet() month-by-month.
#   - Writes with overwrite mode.
#   - Use this once to backfill all historical aggregations.
#
# current (all subsequent scheduled runs):
#   - Uses glueContext.create_dynamic_frame with transformation_ctx so
#     Glue bookmark tracks which curated S3 files have already been processed.
#   - Only new files (added since the last successful job.commit()) are read.
#   - Writes with append mode.
#   - Requires Job bookmark = Enable in the Glue job settings.
# ──────────────────────────────────────────────────────────────────────────────

# ── Job init ───────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Resolve jobtype — defaults to 'historical' if not provided
try:
    _jobtype_args = getResolvedOptions(sys.argv, ['jobtype'])
    JOB_TYPE = _jobtype_args['jobtype'].lower()
except Exception:
    JOB_TYPE = 'historical'
    logger.info("No 'jobtype' parameter provided – defaulting to 'historical'")

logger.info("Job type: %s", JOB_TYPE)

sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")   # suppress Spark/Py4J internal noise from error log
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Spark tuning ───────────────────────────────────────────────────────────────
# shuffle.partitions: aggregation jobs are groupBy-heavy so 200 is appropriate
# for historical. Current mode processes one month — use 8 to avoid 200 nearly
# empty shuffle tasks.
# maxPartitionBytes: 256 MB reduces task count vs 128 MB default.
# partitionOverwriteMode=dynamic ensures only affected partitions are replaced.
if JOB_TYPE == 'historical':
    spark.conf.set("spark.sql.shuffle.partitions", "200")
else:
    spark.conf.set("spark.sql.shuffle.partitions", "8")

spark.conf.set("spark.sql.files.maxPartitionBytes",        "268435456")  # 256 MB
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.filterPushdown",         "true")
spark.conf.set("spark.sql.parquet.mergeSchema",            "false")

# ── Adaptive Query Execution (AQE) ────────────────────────────────────────────
# AQE re-optimises the query plan at runtime using actual partition statistics.
# Key effects:
#   - coalescePartitions: merges small post-shuffle partitions automatically.
#     With shuffle.partitions=200 but only 3 partitions worth of data, AQE
#     collapses 200 tasks → ~3 tasks at the shuffle boundary.
#   - skewJoin: splits skewed partitions to prevent one task doing all the work.
#   - advisoryPartitionSizeInBytes: target size per coalesced partition (256 MB).
#     Spark merges shuffle output until each partition is ~256 MB.
spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",     "268435456")  # 256 MB
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

# ── Config ─────────────────────────────────────────────────────────────────────
account_id      = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET       = f"nyc-tlc-{account_id}"
CURATED_PREFIX  = "curated/hvfhv"
CURATED_PATH    = f"s3://{S3_BUCKET}/{CURATED_PREFIX}/"
ZONE_PATH       = f"s3://{S3_BUCKET}/raw/zone_lookup/taxi_zone_lookup.csv"
AGGREGATED_PATH = f"s3://{S3_BUCKET}/aggregated/"

logger.info("Account ID      : %s", account_id)
logger.info("Curated path    : %s", CURATED_PATH)
logger.info("Zone lookup     : %s", ZONE_PATH)
logger.info("Aggregated path : %s", AGGREGATED_PATH)


# ── Helper: discover monthly partitions (historical mode only) ─────────────────
def list_curated_partitions(bucket: str, prefix: str) -> list:
    """Return a sorted list of (year, month) tuples from hive-style partitions."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    year_prefixes = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            year_prefixes.add(cp["Prefix"])

    partitions = []
    for yp in year_prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=yp, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                parts = cp["Prefix"].rstrip("/").split("/")
                try:
                    y = int(parts[-2].split("=")[1])
                    m = int(parts[-1].split("=")[1])
                    partitions.append((y, m))
                except (IndexError, ValueError):
                    logger.warning("Skipping unexpected prefix: %s", cp["Prefix"])

    return sorted(partitions)


# ── Load zone lookup (small — load once, broadcast to all executors) ───────────
# zone_df is tiny (~300 rows). Loading it once here and broadcasting avoids
# a repeated S3 read on every batch in historical mode.
logger.info("Loading zone lookup table...")
zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ZONE_PATH)
zone_df.cache()
zone_count = zone_df.count()   # materialise cache
logger.info("Loaded %d zone records.", zone_count)

pickup_zones  = zone_df.alias("pu_zone")
dropoff_zones = zone_df.alias("do_zone")


# ── Shared: enrich + aggregate + write one batch ───────────────────────────────
def process_and_write(curated_df, batch_label: str, write_mode: str):
    """Enrich with zones, run all 3 aggregations, write to S3.

    Optimisations vs previous version:
      - curated_df.count() removed — count was only used for logging and
        triggered a full S3 scan before any work was done. The empty-batch
        check is now handled upstream via DynamicFrame.count() (current mode)
        or skipped in historical mode (empty parquet paths raise an exception
        which is caught by the caller).
      - enriched_df.cache() retained — feeds 3 groupBy aggs; without it Spark
        re-executes the join 3 times.
      - borough_agg / zone_agg / service_agg .count() + .show() calls removed
        from the hot path. These triggered 3 extra full scans per batch just
        for diagnostic logging. Replaced with a single post-agg sample log.
      - zone_agg partitionBy("year","month") only — dropped "day" to avoid
        the small-files explosion (same fix applied to raw→curated job).
      - shared_match_flag / wav_match_flag sum: replaced
        sum(when(col == lit(True), lit(1)).otherwise(lit(0)))
        with the simpler sum(col.cast("integer")) — same result, less overhead.
    """

    # ── Coalesce input partitions ──────────────────────────────────────────────
    # curated data is partitioned by year/month/day, so one month of data
    # arrives as 28-31 day directories × N files per day = many small input
    # tasks. Coalescing merges them into a smaller number of larger partitions
    # before the expensive join + 3 groupBy operations, reducing task overhead.
    # Target: ~256 MB per partition. One month ≈ 300-400 MB → 1-2 partitions.
    # Use coalesce (not repartition) — no shuffle, just merges existing tasks.
    if JOB_TYPE == 'current':
        curated_df = curated_df.coalesce(4)   # one month fits in 4 partitions
    # historical mode: already reading month-by-month, coalesce per batch
    else:
        curated_df = curated_df.coalesce(4)

    # ── Enrich with zone info ──────────────────────────────────────────────────
    logger.info("[%s] Enriching data with zone information ...", batch_label)

    enriched_df = curated_df \
        .join(broadcast(pickup_zones),  col("PULocationID") == col("pu_zone.LocationID"), "left") \
        .join(broadcast(dropoff_zones), col("DOLocationID") == col("do_zone.LocationID"), "left") \
        .select(
            col("trip_date"),
            col("day_of_week"),
            col("is_weekend"),
            col("PULocationID"),
            col("DOLocationID"),
            col("shared_request_flag"),
            col("shared_match_flag"),
            col("wav_request_flag"),
            col("wav_match_flag"),
            col("is_airport_trip"),
            col("total_amount"),
            col("trip_miles"),
            col("trip_duration_minutes"),
            col("driver_pay"),
            col("pu_zone.Borough").alias("pickup_borough"),
            col("pu_zone.Zone").alias("pickup_zone"),
            col("pu_zone.service_zone").alias("pickup_service_zone"),
            col("do_zone.Borough").alias("dropoff_borough"),
            col("do_zone.Zone").alias("dropoff_zone"),
            col("do_zone.service_zone").alias("dropoff_service_zone")
        )

    # Cache enriched_df — reused by all 3 aggregations below.
    # No count() here: avoid an extra full scan just for a log line.
    enriched_df.cache()
    logger.info("[%s] Zone enrichment complete.", batch_label)

    # ── Table 1: aggregated_trips_daily_borough ───────────────────────────────
    # Grain: one row per day × pickup borough × dropoff borough × weekend flag
    logger.info("[%s] Building Table 1: aggregated_trips_daily_borough ...", batch_label)

    borough_agg = enriched_df.groupBy(
        "trip_date", "day_of_week", "is_weekend", "pickup_borough", "dropoff_borough"
    ).agg(
        count("*").alias("trip_count"),
        _sum("total_amount").cast("double").alias("total_revenue"),
        _sum("trip_miles").cast("double").alias("total_distance"),
        _sum("trip_duration_minutes").cast("double").alias("total_duration"),
        avg("trip_miles").cast("double").alias("avg_borough_distance"),
        avg("trip_duration_minutes").cast("double").alias("avg_borough_duration")
    ).withColumn("year",  year("trip_date")) \
     .withColumn("month", month("trip_date"))

    # ── Table 2: aggregated_trips_daily_zone ─────────────────────────────────
    # Grain: one row per day × pickup zone ID × dropoff zone ID × airport flag
    logger.info("[%s] Building Table 2: aggregated_trips_daily_zone ...", batch_label)

    zone_agg = enriched_df.groupBy(
        "trip_date", "PULocationID", "DOLocationID", "is_airport_trip"
    ).agg(
        count("*").alias("trip_count"),
        _sum("total_amount").cast("double").alias("total_revenue"),
        avg("trip_miles").cast("double").alias("avg_zone_distance"),
        avg("trip_duration_minutes").cast("double").alias("avg_zone_duration"),
        avg("total_amount").cast("double").alias("avg_fare_per_trip"),
        _sum("driver_pay").cast("double").alias("total_driver_pay"),
        max("pickup_zone").alias("pickup_zone"),
        max("pickup_borough").alias("pickup_borough"),
        max("pickup_service_zone").alias("pickup_service_zone"),
        max("dropoff_zone").alias("dropoff_zone"),
        max("dropoff_borough").alias("dropoff_borough"),
        max("dropoff_service_zone").alias("dropoff_service_zone")
    ).withColumn("year",  year("trip_date")) \
     .withColumn("month", month("trip_date"))

    # ── Table 3: aggregated_trips_daily_service ───────────────────────────────
    # Grain: one row per day × shared flag × WAV flag × weekend flag
    logger.info("[%s] Building Table 3: aggregated_trips_daily_service ...", batch_label)

    service_agg = enriched_df.groupBy(
        "trip_date", "day_of_week", "is_weekend", "shared_request_flag", "wav_request_flag"
    ).agg(
        count("*").alias("trip_count"),
        _sum("total_amount").cast("double").alias("total_revenue"),
        _sum(col("shared_match_flag").cast("integer")).alias("shared_matched_count"),
        _sum(col("wav_match_flag").cast("integer")).alias("wav_matched_count")
    ).withColumn("year",  year("trip_date")) \
     .withColumn("month", month("trip_date"))

    # ── Write all 3 tables ────────────────────────────────────────────────────
    # zone_agg: partitionBy year+month only (dropped "day") — avoids the
    # small-files explosion that year+month+day creates (~30× more files).
    logger.info("[%s] Writing aggregated tables to S3  (mode=%s) ...", batch_label, write_mode)

    t = time.time()
    borough_agg.write.mode(write_mode).partitionBy("year", "month").parquet(f"{AGGREGATED_PATH}daily_borough/")
    logger.info("[%s] Borough aggregation saved  (%.1fs)", batch_label, time.time() - t)

    t = time.time()
    zone_agg.write.mode(write_mode).partitionBy("year", "month").parquet(f"{AGGREGATED_PATH}daily_zone/")
    logger.info("[%s] Zone aggregation saved  (%.1fs)", batch_label, time.time() - t)

    t = time.time()
    service_agg.write.mode(write_mode).partitionBy("year", "month").parquet(f"{AGGREGATED_PATH}daily_service/")
    logger.info("[%s] Service aggregation saved  (%.1fs)", batch_label, time.time() - t)

    # Release enriched_df — all 3 aggs are written
    enriched_df.unpersist()

    logger.info("[%s] Batch complete.", batch_label)


# ═══════════════════════════════════════════════════════════════════════════════
# HISTORICAL MODE — month-by-month full backfill
# bypasses bookmark; writes with overwrite
# ═══════════════════════════════════════════════════════════════════════════════
job_start       = time.time()
batches_ok      = 0
batches_skipped = 0

if JOB_TYPE == 'historical':
    logger.info("=" * 60)
    logger.info("HISTORICAL MODE — full backfill via partition loop")
    logger.info("Bookmark is NOT used. All curated partitions will be processed.")
    logger.info("=" * 60)

    all_partitions = list_curated_partitions(S3_BUCKET, CURATED_PREFIX)

    if not all_partitions:
        logger.error("No monthly partitions found under %s – aborting.", CURATED_PATH)
        job.commit()
        # Do NOT use sys.exit() — Glue treats SystemExit as a failure.
        # Fall through to job.commit() below; the error log makes the issue clear.
    else:
        logger.info("Discovered %d partition(s) to process.", len(all_partitions))

        for yr, mn in all_partitions:
            chunk_label = f"{yr}-{mn:02d}"
            month_path  = f"{CURATED_PATH}year={yr}/month={mn}/"
            logger.info("=== Processing chunk %s ===", chunk_label)
            logger.info("Reading from: %s", month_path)

            try:
                curated_df = spark.read.parquet(month_path)
            except Exception as e:
                logger.warning("Skipping chunk %s: %s", chunk_label, e)
                batches_skipped += 1
                continue

            process_and_write(curated_df, chunk_label, write_mode="overwrite")
            batches_ok += 1
            logger.info("Chunk %s complete. Batches done: %d", chunk_label, batches_ok)

# ═══════════════════════════════════════════════════════════════════════════════
# CURRENT MODE — bookmark-based incremental read
# uses DynamicFrame + transformation_ctx; writes with append
# ═══════════════════════════════════════════════════════════════════════════════
else:
    logger.info("=" * 60)
    logger.info("CURRENT MODE — incremental via Glue job bookmark")
    logger.info("Only files added since last job.commit() will be read.")
    logger.info("Requires Job bookmark = Enable in Glue job settings.")
    logger.info("=" * 60)

    # Register any new curated partitions written by the raw→curated job
    # before this job reads them.
    logger.info("Running MSCK REPAIR TABLE on nyc_tlc_db.curated_hvfhv ...")
    try:
        spark.sql("MSCK REPAIR TABLE nyc_tlc_db.curated_hvfhv")
        logger.info("MSCK REPAIR TABLE nyc_tlc_db.curated_hvfhv complete.")
    except Exception as e:
        logger.warning("MSCK REPAIR TABLE failed (continuing): %s", e)

    # Use DynamicFrame.count() to short-circuit before converting to DataFrame.
    # This avoids curated_df.cache() + count() + unpersist() when there is
    # nothing new to process, and — critically — avoids sys.exit(0) which
    # Glue's process launcher catches as an exception and marks as FAILED.
    curated_dyf = glueContext.create_dynamic_frame.from_catalog(
        database="nyc_tlc_db",
        table_name="curated_hvfhv",
        transformation_ctx="curated_hvfhv_source",   # ← bookmark key
    )

    dyf_count = curated_dyf.count()
    logger.info("New records detected by bookmark: %s", f"{dyf_count:,}")

    if dyf_count == 0:
        logger.info("No new curated data since last run — bookmark is up to date.")
    else:
        # Convert to DataFrame only if there is data to process
        curated_df = curated_dyf.toDF()
        process_and_write(curated_df, "bookmark_run", write_mode="append")
        batches_ok = 1

# ── Job-level summary ──────────────────────────────────────────────────────────
job_secs = time.time() - job_start
job_mins = job_secs / 60

logger.info("=" * 60)
logger.info("JOB SUMMARY  (jobtype=%s)", JOB_TYPE)
logger.info("  Batches processed      : %d", batches_ok)
logger.info("  Batches skipped/failed : %d", batches_skipped)
logger.info("  Total job duration     : %.1f seconds (%.1f minutes)", job_secs, job_mins)
logger.info("=" * 60)

logger.info("Output locations:")
logger.info("  Borough : %sdaily_borough/", AGGREGATED_PATH)
logger.info("  Zone    : %sdaily_zone/",    AGGREGATED_PATH)
logger.info("  Service : %sdaily_service/", AGGREGATED_PATH)

# ── MSCK REPAIR — sync Glue catalog with new aggregated partitions ─────────────
logger.info("Running MSCK REPAIR TABLE on aggregated tables ...")
for tbl in [
    "nyc_tlc_db.aggregated_daily_borough",
    "nyc_tlc_db.aggregated_daily_zone",
    "nyc_tlc_db.aggregated_daily_service",
]:
    try:
        spark.sql(f"MSCK REPAIR TABLE {tbl}")
        logger.info("MSCK REPAIR TABLE %s complete.", tbl)
    except Exception as e:
        logger.warning("MSCK REPAIR TABLE %s failed (non-fatal): %s", tbl, e)

# Commit — advances the bookmark for current mode;
# required to finalise the job for historical mode.
logger.info("Committing job.")
job.commit()
