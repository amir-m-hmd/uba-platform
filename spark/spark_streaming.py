import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/uba_metadata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "uba_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "uba_password_2026")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/uba_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse_admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass_2026")

def create_spark_session():
    return SparkSession.builder \
        .appName("UBA-Stream-Processor") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hadoop") \
        .config("spark.sql.catalog.demo.warehouse", "/tmp/iceberg_warehouse") \
        .config("spark.sql.catalog.demo.cache-enabled", "false") \
        .getOrCreate()

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("attributes", StructType([
        StructField("item_category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("search_query", StringType(), True)
    ]), True)
])

def init_iceberg_table(spark):
    """Ensures Iceberg database and table exist before streaming."""
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.uba_db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.uba_db.user_events_lakehouse (
            event_id STRING,
            user_id STRING,
            username STRING,
            email STRING,
            country STRING,
            city STRING,
            device_type STRING,
            is_premium BOOLEAN,
            session_id STRING,
            event_type STRING,
            page_url STRING,
            referrer STRING,
            ip_address STRING,
            event_time TIMESTAMP,
            item_category STRING,
            price DOUBLE,
            search_query STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time), event_type)
    """)

def process_batch(df, batch_id):
    if df.isEmpty():
        return

    spark = df.sparkSession
    
    # 1. Fetch Metadata from PostgreSQL
    try:
        users_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "users") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    except Exception as e:
        print(f"❌ Failed to fetch Postgres Metadata in batch {batch_id}: {e}")
        return

    # 2. Join Stream with Metadata
    enriched_df = df.join(users_df, "user_id", "left") \
        .select(
            col("event_id"),
            col("user_id"),
            col("username"),
            col("email"),
            col("country"),
            col("city"),
            col("device_type"),
            col("is_premium"),
            col("session_id"),
            col("event_type"),
            col("page_url"),
            col("referrer"),
            col("ip_address"),
            col("timestamp").cast(TimestampType()).alias("event_time"),
            col("attributes.item_category").alias("item_category"),
            col("attributes.price").alias("price"),
            col("attributes.search_query").alias("search_query"),
            current_timestamp().alias("processed_at")
        )

    enriched_df.persist()

    # 3. Sink A: ClickHouse DW
    try:
        enriched_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/uba_analytics") \
            .option("dbtable", "user_events_enriched") \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "1000") \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"⚠️ ClickHouse Sink Error (Batch {batch_id}): {e}")

    # 4. Sink B: Apache Iceberg Lakehouse
    try:
        enriched_df.writeTo("demo.uba_db.user_events_lakehouse").append()
    except Exception as e:
        print(f"⚠️ Iceberg Lakehouse Sink Error (Batch {batch_id}): {e}")

    enriched_df.unpersist()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("⚡ Initializing Apache Iceberg Table...")
    init_iceberg_table(spark)

    print("⚡ Starting Spark Structured Streaming Consumer...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), event_schema).alias("data")) \
        .select("data.*")

    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/uba_events") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()