import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get account ID and configure paths
account_id = boto3.client('sts').get_caller_identity()['Account']
S3_BUCKET = f"mission-deh-hof-nyc-tlc-{account_id}"
CURATED_PATH = f"s3://{S3_BUCKET}/curated/hvfhv/"
ZONE_PATH = f"s3://{S3_BUCKET}/raw/zone_lookup/taxi_zone_lookup.csv"
AGGREGATED_PATH = f"s3://{S3_BUCKET}/aggregated/"

print(f"Account ID: {account_id}")
print(f"Processing all curated HVFHV data...")
print(f"Curated data: {CURATED_PATH}")
print(f"Zone lookup: {ZONE_PATH}")
print(f"Aggregated output: {AGGREGATED_PATH}")

# Load all curated HVFHV data
print(f"📊 Loading all curated HVFHV data...")
curated_df = spark.read.parquet(CURATED_PATH)
print(f"✅ Loaded {curated_df.count():,} curated records")

# Load zone lookup table
print("\n🗺️ Loading zone lookup table...")
zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ZONE_PATH)
print(f"✅ Loaded {zone_df.count():,} zone records")

# Show schemas
print("\n📋 Curated Schema:")
curated_df.printSchema()

print("\n📋 Zone Schema:")
zone_df.printSchema()

# Sample data
print("\n🔍 Sample Curated Data:")
curated_df.select("trip_date", "PULocationID", "DOLocationID", "total_amount", "is_weekend").show(5)

print("\n🔍 Sample Zone Data:")
zone_df.show(5)
# Join curated data with zone information for both pickup and dropoff
print("🔗 Enriching data with zone information...")

# Alias zone tables for pickup and dropoff joins
pickup_zones = zone_df.alias("pu_zone")
dropoff_zones = zone_df.alias("do_zone")

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

print(f"✅ Enriched dataset created with {enriched_df.count():,} records")
print("\n🔍 Sample Enriched Data:")
enriched_df.select("trip_date", "pickup_borough", "dropoff_borough", "total_amount").show(5)
print("📊 Creating Table 1: aggregated_trips_daily_borough")
print("Grain: One row per day × pickup borough × dropoff borough × weekend flag")

# Aggregate by borough level
borough_agg = enriched_df.groupBy(
    "trip_date",
    "day_of_week", 
    "is_weekend",
    "pickup_borough",
    "dropoff_borough"
).agg(
    # Core metrics
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    sum("trip_miles").cast("decimal(12,2)").alias("total_distance"),
    sum("trip_duration_minutes").cast("bigint").alias("total_duration"),
    
    # Calculated metrics
    avg("trip_miles").cast("decimal(10,2)").alias("avg_borough_distance"),
    avg("trip_duration_minutes").cast("decimal(10,2)").alias("avg_borough_duration")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

print(f"✅ Borough aggregation created with {borough_agg.count():,} records")
print("\n🔍 Sample Borough Aggregation:")
borough_agg.orderBy(desc("total_revenue")).show(10, truncate=False)
print("📊 Creating Table 2: aggregated_trips_daily_zone")
print("Grain: One row per day × pickup zone ID × dropoff zone ID × airport flag")

# Aggregate by zone level with denormalized zone names
zone_agg = enriched_df.groupBy(
    "trip_date",
    "PULocationID",
    "DOLocationID", 
    "is_airport_trip"
).agg(
    # Core metrics
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

print(f"✅ Zone aggregation created with {zone_agg.count():,} records")
print("\n🔍 Sample Zone Aggregation (Top Routes by Revenue):")
zone_agg.orderBy(desc("total_revenue")).show(10, truncate=False)
print("📊 Creating Table 3: aggregated_trips_daily_service")
print("Grain: One row per day × shared flag × WAV flag × weekend flag")

# Aggregate by service type (simplified)
service_agg = enriched_df.groupBy(
    "trip_date",
    "day_of_week",
    "is_weekend",
    "shared_request_flag",
    "wav_request_flag"
).agg(
    # Core metrics
    count("*").alias("trip_count"),
    sum("total_amount").cast("decimal(12,2)").alias("total_revenue"),
    
    # Service-specific metrics
    sum(when(col("shared_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("shared_matched_count"),
    sum(when(col("wav_match_flag") == lit(True), lit(1)).otherwise(lit(0))).alias("wav_matched_count")
).withColumn("year", year("trip_date")) \
 .withColumn("month", month("trip_date"))

print(f"✅ Service aggregation created with {service_agg.count():,} records")
print("\n🔍 Sample Service Aggregation:")
service_agg.orderBy("trip_date", "shared_request_flag", "wav_request_flag").show(10, truncate=False)
print("💾 Writing aggregated tables to S3...")

# Write Table 1: Borough Summary (partitioned by year, month)
print("\n📊 Writing aggregated_trips_daily_borough...")
borough_agg.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{AGGREGATED_PATH}daily_borough/")
print("✅ Borough aggregation saved")

# Write Table 2: Zone Summary (partitioned by year, month, day)
print("\n📊 Writing aggregated_trips_daily_zone...")
zone_agg.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{AGGREGATED_PATH}daily_zone/")
print("✅ Zone aggregation saved")

# Write Table 3: Service Summary (partitioned by year, month)
print("\n📊 Writing aggregated_trips_daily_service...")
service_agg.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{AGGREGATED_PATH}daily_service/")
print("✅ Service aggregation saved")

print("\n🎉 All aggregation tables successfully created!")
print("📊 AGGREGATION PIPELINE SUMMARY - ALL DATA")
print("=" * 50)

# Validate record counts
original_count = curated_df.count()
borough_count = borough_agg.count()
zone_count = zone_agg.count()
service_count = service_agg.count()

print(f"📈 Data Compression Analysis:")
print(f"Original curated records: {original_count:,}")
print(f"Borough aggregation: {borough_count:,} ({(borough_count/original_count)*100:.4f}% of original)")
print(f"Zone aggregation: {zone_count:,} ({(zone_count/original_count)*100:.2f}% of original)")
print(f"Service aggregation: {service_count:,} ({(service_count/original_count)*100:.4f}% of original)")

print(f"\n📂 Output Locations:")
print(f"Borough: {AGGREGATED_PATH}daily_borough/")
print(f"Zone: {AGGREGATED_PATH}daily_zone/")
print(f"Service: {AGGREGATED_PATH}daily_service/")

print(f"\n🎯 Business Use Cases Enabled:")
print("✅ Borough: Weekend vs weekday revenue analysis")
print("✅ Zone: Route optimization and driver deployment")
print("✅ Service: Accessibility compliance and shared ride trends")

print(f"\n✅ Curated to Aggregated pipeline completed successfully!")

job.commit()