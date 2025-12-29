from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import logging

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)


PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.75.0",
    "org.apache.hadoop:hadoop-aws:3.3.4" 
]

spark = SparkSession.builder \
    .appName("BinanceToIceberg") \
    .config("spark.jars.packages", ",".join(PACKAGES)) \
    .config("spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://lake-bucket") \
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("trade_id", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True)
])

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_trades") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and select fields
df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to Iceberg (Nessie catalog)
try:
    query = df_parsed.writeStream \
        .format("iceberg") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "./checkpoint_dir") \
        .toTable("nessie.binance_trades")

    logging.info("Spark job started, writing to Iceberg table 'nessie.binance_trades'.")
    query.awaitTermination()
except KeyboardInterrupt:
    logging.info("Job stopped by user (KeyboardInterrupt).")
except Exception as e:
    logging.error(f"Job failed with error: {e}")
finally:
    if 'query' in locals() and query.isActive:
        query.stop()
        logging.info("Streaming query stopped.")
    
    spark.stop()
    logging.info("Spark session closed.")