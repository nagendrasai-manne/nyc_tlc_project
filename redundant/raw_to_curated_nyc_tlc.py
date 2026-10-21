import sys
import time
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ──────────────────────────────────────────────
# Logger setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

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
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Tune Spark for large parquet workloads
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")   # 128 MB per partition
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
account_id    = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET     = f"nyc-tlc-{account_id}"
RAW_BASE_PATH = f"s3://{S3_BUCKET}/raw/hvfhv"
CURATED_PATH  = f"s3://{S3_BUCKET}/curated/hvfhv/"

VALID_LICENSES = ['HV0002', 'HV0003', 'HV0004', 'HV0005']


# ──────────────────────────────────────────────
# List available year/month partitions in raw/
# ──────────────────────────────────────────────
def list_raw_partitions(s3_client, bucket: str, prefix: str) -> list[tuple[int, int]]:
    """Return a sorted list of (year, month) tuples from hive-style partitions
    under raw/hvfhv/year=YYYY/month=MM/.
    Falls back to a single-batch run if no partitions are found.
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
# Core transformation
# ──────────────────────────────────────────────
def transform_raw_to_curated(raw_df):
    """Transform raw HVFHV data to curated layer."""

    curated_df = raw_df.select(
        # Passthrough fields
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),

        # Timestamps
        col("request_datetime").cast("timestamp").alias("request_datetime"),
        col("on_scene_datetime").cast("timestamp").alias("on_scene_datetime"),
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),

        # Location IDs
        col("PULocationID").cast("integer").alias("PULocationID"),
        col("DOLocationID").cast("integer").alias("DOLocationID"),

        # Numeric fields with null-safe coalesce
        coalesce(col("trip_miles"),           lit(0.0)).cast("decimal(10,2)").alias("trip_miles"),
        col("trip_time").cast("integer").alias("trip_time_seconds"),
        coalesce(col("base_passenger_fare"),  lit(0.0)).cast("decimal(10,2)").alias("base_passenger_fare"),
        coalesce(col("tolls"),                lit(0.0)).cast("decimal(10,2)").alias("tolls"),
        coalesce(col("bcf"),                  lit(0.0)).cast("decimal(10,2)").alias("bcf"),
        coalesce(col("sales_tax"),            lit(0.0)).cast("decimal(10,2)").alias("sales_tax"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("decimal(10,2)").alias("congestion_surcharge"),
        coalesce(col("airport_fee"),          lit(0.0)).cast("decimal(10,2)").alias("airport_fee"),
        coalesce(col("tips"),                 lit(0.0)).cast("decimal(10,2)").alias("tips"),
        coalesce(col("driver_pay"),           lit(0.0)).cast("decimal(10,2)").alias("driver_pay"),

        # Boolean flag conversions (Y/N → TRUE/FALSE)
        when(col("shared_request_flag") == "Y", True).otherwise(False).alias("shared_request_flag"),
        when(col("shared_match_flag")    == "Y", True).otherwise(False).alias("shared_match_flag"),
        when(col("access_a_ride_flag")   == "Y", True).otherwise(False).alias("access_a_ride_flag"),
        when(col("wav_request_flag")     == "Y", True).otherwise(False).alias("wav_request_flag"),
        when(col("wav_match_flag")       == "Y", True).otherwise(False).alias("wav_match_flag"),

        # New field (Jan 5 2025+)
        lit(0.0).cast("decimal(10,2)").alias("cbd_congestion_fee"),

        # Derived date/time fields
        col("pickup_datetime").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),

        when(dayofweek("pickup_datetime") == 1, "Sunday")
        .when(dayofweek("pickup_datetime") == 2, "Monday")
        .when(dayofweek("pickup_datetime") == 3, "Tuesday")
        .when(dayofweek("pickup_datetime") == 4, "Wednesday")
        .when(dayofweek("pickup_datetime") == 5, "Thursday")
        .when(dayofweek("pickup_datetime") == 6, "Friday")
        .when(dayofweek("pickup_datetime") == 7, "Saturday")
        .otherwise("Unknown").alias("day_of_week"),

        when(dayofweek("pickup_datetime").isin([1, 7]), True).otherwise(False).alias("is_weekend"),

        (col("trip_time") / 60.0).cast("decimal(10,2)").alias("trip_duration_minutes"),

        # Total fare (base + fees, excluding tips)
        (coalesce(col("base_passenger_fare"),  lit(0.0)) +
         coalesce(col("tolls"),                lit(0.0)) +
         coalesce(col("bcf"),                  lit(0.0)) +
         coalesce(col("sales_tax"),            lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"),          lit(0.0)) +
         lit(0.0)   # cbd_congestion_fee
        ).cast("decimal(10,2)").alias("total_fare"),

        # Total amount (total_fare + tips)
        (coalesce(col("base_passenger_fare"),  lit(0.0)) +
         coalesce(col("tolls"),                lit(0.0)) +
         coalesce(col("bcf"),                  lit(0.0)) +
         coalesce(col("sales_tax"),            lit(0.0)) +
         coalesce(col("congestion_surcharge"), lit(0.0)) +
         coalesce(col("airport_fee"),          lit(0.0)) +
         lit(0.0) +  # cbd_congestion_fee
         coalesce(col("tips"),                 lit(0.0))
        ).cast("decimal(10,2)").alias("total_amount"),

        # Average speed (guarded against division by zero)
        when(col("trip_time") > 0,
             (col("trip_miles") / (col("trip_time") / 3600.0))
        ).otherwise(None).cast("decimal(10,2)").alias("avg_speed_mph"),

        when(coalesce(col("airport_fee"), lit(0.0)) > 0, True).otherwise(False).alias("is_airport_trip"),

        # Partition columns
        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"),
        dayofmonth("pickup_datetime").alias("day"),

        current_timestamp().alias("processing_timestamp")
    )

    return curated_df


# ──────────────────────────────────────────────
# Rejection breakdown — one aggregation pass
# ──────────────────────────────────────────────
def log_rejection_breakdown(curated_df, total: int, batch_label: str):
    """Count how many records fail each individual validation rule."""

    rules = {
        "null pickup_datetime"   : curated_df.filter(col("pickup_datetime").isNull()),
        "null dropoff_datetime"  : curated_df.filter(col("dropoff_datetime").isNull()),
        "null PULocationID"      : curated_df.filter(col("PULocationID").isNull()),
        "null DOLocationID"      : curated_df.filter(col("DOLocationID").isNull()),
        "trip_miles out of range": curated_df.filter(~((col("trip_miles") >= 0) & (col("trip_miles") <= 500))),
        "fare out of range"      : curated_df.filter(~((col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 10000))),
        "dropoff <= pickup"      : curated_df.filter(~(col("dropoff_datetime") > col("pickup_datetime"))),
        "pickup before request"  : curated_df.filter(~(col("pickup_datetime") >= col("request_datetime"))),
        "invalid license"        : curated_df.filter(~col("hvfhs_license_num").isin(VALID_LICENSES)),
    }

    lines = [f"Rejection breakdown — {batch_label}  (total rows: {total:,})"]
    lines.append(f"  {'Rule':<30} {'Rejected':>10}  {'%':>7}")
    lines.append("  " + "-" * 52)
    for rule_name, rule_df in rules.items():
        n = rule_df.count()
        pct = (n / total * 100) if total else 0.0
        lines.append(f"  {rule_name:<30} {n:>10,}  {pct:>6.2f}%")

    logger.info("\n".join(lines))


# ──────────────────────────────────────────────
# Data quality validation
# ──────────────────────────────────────────────
def validate_curated_data(curated_df):
    """Apply data-quality filters and return the cleaned DataFrame."""

    validated_df = curated_df.filter(
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

    return validated_df


# ──────────────────────────────────────────────
# License distribution log
# ──────────────────────────────────────────────
def log_license_distribution(df, batch_label: str):
    """Log record count per hvfhs_license_num."""
    rows = (
        df.groupBy("hvfhs_license_num")
          .count()
          .orderBy(desc("count"))
          .collect()
    )
    lines = [f"License distribution — {batch_label}"]
    lines.append(f"  {'License':<20} {'Count':>12}")
    lines.append("  " + "-" * 34)
    for row in rows:
        lines.append(f"  {row['hvfhs_license_num']:<20} {row['count']:>12,}")
    logger.info("\n".join(lines))


# ──────────────────────────────────────────────
# Main processing loop — one month at a time
# ──────────────────────────────────────────────
job_start = time.time()

s3_client = boto3.client("s3")
partitions = list_raw_partitions(s3_client, S3_BUCKET, "raw/hvfhv")

if not partitions:
    logger.warning("No year/month partitions found. Processing %s/ as a single batch.", RAW_BASE_PATH)
    partitions = [(None, None)]

logger.info("Discovered %d partition(s) in S3.", len(partitions))

# ── Filter partitions based on jobtype ────────────────────────────────────────
if JOB_TYPE == 'current':
    # current mode: process only the latest (most recent) partition
    partitions = [partitions[-1]]
    logger.info("current mode → processing latest partition only: %s-%02d", partitions[0][0], partitions[0][1])
else:
    # historical mode: process all discovered partitions (default)
    logger.info("historical mode → processing all %d partition(s).", len(partitions))

logger.info("Partitions to process: %s", partitions)

total_written  = 0
total_rejected = 0
batches_ok     = 0
batches_skipped = 0

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

    # Cache once; reused by transform → validate → write
    raw_df.cache()

    row_count = raw_df.count()
    logger.info("Rows loaded: %s   Columns: %d", f"{row_count:,}", len(raw_df.columns))

    if row_count == 0:
        logger.warning("Empty batch %s — skipping.", batch_label)
        raw_df.unpersist()
        batches_skipped += 1
        continue

    # ── Transform ──────────────────────────────
    curated_df = transform_raw_to_curated(raw_df)
    raw_df.unpersist()  # raw no longer needed after transform

    # ── Rejection breakdown (1 — per rule counts) ──
    log_rejection_breakdown(curated_df, row_count, batch_label)

    # ── License distribution (2 — on raw curated before filtering) ──
    log_license_distribution(curated_df, batch_label)

    # ── Validate ───────────────────────────────
    validated_df = validate_curated_data(curated_df)

    validated_df.cache()
    validated_count = validated_df.count()
    rejected_count  = row_count - validated_count
    rejection_rate  = (rejected_count / row_count * 100) if row_count else 0.0

    logger.info("Valid   : %s", f"{validated_count:,}")
    logger.info("Rejected: %s  (%.2f%%)", f"{rejected_count:,}", rejection_rate)

    # ── Write ──────────────────────────────────
    write_start = time.time()
    logger.info("Writing to: %s", CURATED_PATH)

    try:
        validated_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(CURATED_PATH)
    except Exception as e:
        logger.error("Write failed for batch %s: %s", batch_label, e, exc_info=True)
        validated_df.unpersist()
        batches_skipped += 1
        continue

    write_secs = time.time() - write_start
    logger.info("Write completed in %.1f seconds.", write_secs)

    total_written  += validated_count
    total_rejected += rejected_count
    batches_ok     += 1
    logger.info("Batch %s done.  Running total written: %s", batch_label, f"{total_written:,}")

    validated_df.unpersist()


# ──────────────────────────────────────────────
# Job-level summary
# ──────────────────────────────────────────────
job_secs = time.time() - job_start
job_mins = job_secs / 60

logger.info("=" * 60)
logger.info("JOB SUMMARY")
logger.info("  Total batches discovered : %d", len(partitions))
logger.info("  Batches processed        : %d", batches_ok)
logger.info("  Batches skipped/failed   : %d", batches_skipped)
logger.info("  Total records written    : %s", f"{total_written:,}")
logger.info("  Total records rejected   : %s", f"{total_rejected:,}")
logger.info("  Total job duration       : %.1f seconds (%.1f minutes)", job_secs, job_mins)
logger.info("=" * 60)

# ──────────────────────────────────────────────
# MSCK REPAIR — sync Glue catalog partitions
# ──────────────────────────────────────────────
# Registers any new year/month/day partitions written above so Athena
# can query them immediately without running a full Glue crawler.
logger.info("Running MSCK REPAIR TABLE on nyc_tlc_db.curated_hvfhv ...")
spark.sql("MSCK REPAIR TABLE nyc_tlc_db.curated_hvfhv")
logger.info("MSCK REPAIR TABLE complete.")

job.commit()
