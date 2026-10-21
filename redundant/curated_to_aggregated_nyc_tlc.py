import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("curated_to_aggregated")

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
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Config ─────────────────────────────────────────────────────────────────────
account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET        = f"nyc-tlc-{account_id}"
CURATED_PREFIX   = "curated/hvfhv"
CURATED_PATH     = f"s3://{S3_BUCKET}/{CURATED_PREFIX}/"
ZONE_PATH        = f"s3://{S3_BUCKET}/raw/zone_lookup/taxi_zone_lookup.csv"
AGGREGATED_PATH  = f"s3://{S3_BUCKET}/aggregated/"

logger.info("Account ID: %s", account_id)
logger.info("Processing all curated HVFHV data...")
logger.info("Curated data: %s", CURATED_PATH)
logger.info("Zone lookup: %s", ZONE_PATH)
logger.info("Aggregated output: %s", AGGREGATED_PATH)


# ── Helper: discover monthly partitions ───────────────────────────────────────
def list_curated_partitions(bucket: str, prefix: str) -> list:
    """
    Walk s3://<bucket>/<prefix> and return a sorted list of (year, month) tuples
    found as year=YYYY/month=M sub-prefixes.
    """
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


# ── Load zone lookup (small – load once) ──────────────────────────────────────
logger.info("Loading zone lookup table...")
zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ZONE_PATH)
logger.info("Loaded %d zone records", zone_df.count())

zone_df.printSchema()
zone_df.show(5)

pickup_zones  = zone_df.alias("pu_zone")
dropoff_zones = zone_df.alias("do_zone")

# ── Discover and filter partitions ────────────────────────────────────────────
logger.info("Discovering monthly partitions in %s ...", CURATED_PATH)
all_partitions = list_curated_partitions(S3_BUCKET, CURATED_PREFIX)

if not all_partitions:
    logger.error("No monthly partitions found under %s – aborting.", CURATED_PATH)
    job.commit()
    sys.exit(1)

logger.info("Found %d monthly partition(s).", len(all_partitions))

if JOB_TYPE == 'current':
    # current mode: only the latest (most recent) partition
    partitions = [all_partitions[-1]]
    logger.info("current mode → processing latest partition only: %d-%02d", partitions[0][0], partitions[0][1])
else:
    # historical mode: all partitions (default)
    partitions = all_partitions
    logger.info("historical mode → processing all %d partition(s).", len(partitions))

logger.info("Partitions to process: %s", partitions)

# ── Main loop – one month at a time ───────────────────────────────────────────
for yr, mn in partitions:
    chunk_label = f"{yr}-{mn:02d}"
    month_path  = f"{CURATED_PATH}year={yr}/month={mn}/"
    logger.info("=== Processing chunk %s ===", chunk_label)
    logger.info("Reading from: %s", month_path)

    # Load all curated HVFHV data for this month
    logger.info("Loading curated HVFHV data for %s...", chunk_label)
    curated_df = spark.read.parquet(month_path)
    logger.info("Loaded %d curated records", curated_df.count())

    logger.info("Curated Schema:")
    curated_df.printSchema()

    logger.info("Sample Curated Data:")
    curated_df.select("trip_date", "PULocationID", "DOLocationID", "total_amount", "is_weekend").show(5)

    # Join curated data with zone information for both pickup and dropoff
    logger.info("Enriching data with zone information...")

    # Create enriched dataset with denormalized zone information
    enriched_df = curated_df \
        .join(pickup_zones, col("PULocationID") == col("pu_zone.LocationID"), "left") \
        .join(dropoff_zones, col("DOLocationID") == col("do_zone.LocationID"), "left") \
        .select(
            # Core trip data
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

            # Financial metrics
            col("total_amount"),
            col("trip_miles"),
            col("trip_duration_minutes"),
            col("driver_pay"),

            # Pickup zone information (denormalized)
            col("pu_zone.Borough").alias("pickup_borough"),
            col("pu_zone.Zone").alias("pickup_zone"),
            col("pu_zone.service_zone").alias("pickup_service_zone"),

            # Dropoff zone information (denormalized)
            col("do_zone.Borough").alias("dropoff_borough"),
            col("do_zone.Zone").alias("dropoff_zone"),
            col("do_zone.service_zone").alias("dropoff_service_zone")
        )

    logger.info("Enriched dataset created with %d records", enriched_df.count())
    logger.info("Sample Enriched Data:")
    enriched_df.select("trip_date", "pickup_borough", "dropoff_borough", "total_amount").show(5)

    # ── Table 1: aggregated_trips_daily_borough ────────────────────────────────
    logger.info("Creating Table 1: aggregated_trips_daily_borough")
    logger.info("Grain: One row per day x pickup borough x dropoff borough x weekend flag")

    borough_agg = enriched_df.groupBy(
        "trip_date",
        "day_of_week",
        "is_weekend",
        "pickup_borough",
        "dropoff_borough"
    ).agg(
        count("*").alias("trip_count"),
        sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
        sum("trip_miles").cast("decimal(12,2)").alias("total_distance"),
        sum("trip_duration_minutes").cast("bigint").alias("total_duration"),
        avg("trip_miles").cast("decimal(10,2)").alias("avg_borough_distance"),
        avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_borough_duration")
    ).withColumn("year", year("trip_date")) \
     .withColumn("month", month("trip_date"))

    logger.info("Borough aggregation created with %d records", borough_agg.count())
    logger.info("Sample Borough Aggregation:")
    borough_agg.orderBy(desc("total_revenue")).show(10, truncate=False)

    # ── Table 2: aggregated_trips_daily_zone ──────────────────────────────────
    logger.info("Creating Table 2: aggregated_trips_daily_zone")
    logger.info("Grain: One row per day x pickup zone ID x dropoff zone ID x airport flag")

    zone_agg = enriched_df.groupBy(
        "trip_date",
        "PULocationID",
        "DOLocationID",
        "is_airport_trip"
    ).agg(
        count("*").alias("trip_count"),
        sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
        avg("trip_miles").cast("decimal(10,2)").alias("avg_zone_distance"),
        avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_zone_duration"),
        avg("total_amount").cast("decimal(10,2)").alias("avg_fare_per_trip"),
        sum("driver_pay").cast("decimal(12,2)").alias("total_driver_pay"),

        # Denormalized zone information (using MAX since all records in group have same values)
        max("pickup_zone").alias("pickup_zone"),
        max("pickup_borough").alias("pickup_borough"),
        max("pickup_service_zone").alias("pickup_service_zone"),
        max("dropoff_zone").alias("dropoff_zone"),
        max("dropoff_borough").alias("dropoff_borough"),
        max("dropoff_service_zone").alias("dropoff_service_zone")
    ).withColumn("year", year("trip_date")) \
     .withColumn("month", month("trip_date")) \
     .withColumn("day", dayofmonth("trip_date"))

    logger.info("Zone aggregation created with %d records", zone_agg.count())
    logger.info("Sample Zone Aggregation (Top Routes by Revenue):")
    zone_agg.orderBy(desc("total_revenue")).show(10, truncate=False)

    # ── Table 3: aggregated_trips_daily_service ───────────────────────────────
    logger.info("Creating Table 3: aggregated_trips_daily_service")
    logger.info("Grain: One row per day x shared flag x WAV flag x weekend flag")

    service_agg = enriched_df.groupBy(
        "trip_date",
        "day_of_week",
        "is_weekend",
        "shared_request_flag",
        "wav_request_flag"
    ).agg(
        count("*").alias("trip_count"),
        sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
        sum(when(col("shared_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("shared_matched_count"),
        sum(when(col("wav_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("wav_matched_count")
    ).withColumn("year", year("trip_date")) \
     .withColumn("month", month("trip_date"))

    logger.info("Service aggregation created with %d records", service_agg.count())
    logger.info("Sample Service Aggregation:")
    service_agg.orderBy("trip_date", "shared_request_flag", "wav_request_flag").show(10, truncate=False)

    # ── Write ─────────────────────────────────────────────────────────────────
    logger.info("Writing aggregated tables to S3 for chunk %s...", chunk_label)

    logger.info("Writing aggregated_trips_daily_borough...")
    borough_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{AGGREGATED_PATH}daily_borough/")
    logger.info("Borough aggregation saved")

    logger.info("Writing aggregated_trips_daily_zone...")
    zone_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"{AGGREGATED_PATH}daily_zone/")
    logger.info("Zone aggregation saved")

    logger.info("Writing aggregated_trips_daily_service...")
    service_agg.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{AGGREGATED_PATH}daily_service/")
    logger.info("Service aggregation saved")

    logger.info("Chunk %s complete.", chunk_label)

# ── Final summary ──────────────────────────────────────────────────────────────
logger.info("All aggregation tables successfully created!")
logger.info("AGGREGATION PIPELINE SUMMARY - ALL DATA")
logger.info("=" * 50)

logger.info("Output Locations:")
logger.info("Borough: %sdaily_borough/", AGGREGATED_PATH)
logger.info("Zone: %sdaily_zone/", AGGREGATED_PATH)
logger.info("Service: %sdaily_service/", AGGREGATED_PATH)

logger.info("Business Use Cases Enabled:")
logger.info("Borough: Weekend vs weekday revenue analysis")
logger.info("Zone: Route optimization and driver deployment")
logger.info("Service: Accessibility compliance and shared ride trends")

# ── MSCK REPAIR — sync Glue catalog partitions ────────────────────────────────
# Registers any new year/month partitions written above so Athena
# can query them immediately without running a full Glue crawler.
logger.info("Running MSCK REPAIR TABLE on aggregated tables ...")
spark.sql("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_borough")
logger.info("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_borough complete.")
spark.sql("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_zone")
logger.info("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_zone complete.")
spark.sql("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_service")
logger.info("MSCK REPAIR TABLE nyc_tlc_db.aggregated_daily_service complete.")

logger.info("Curated to Aggregated pipeline completed successfully!")

job.commit()
