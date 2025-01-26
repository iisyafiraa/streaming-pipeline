import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, col, avg, window, sum, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_CONTAINER_NAME')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DW_DB')}"
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
print(f"postgres env: {postgres_url}, {postgres_user}, {postgres_password}")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

spark = (
    pyspark.sql.SparkSession.builder.appName("StreamingTest")
    .master(spark_host)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = stream_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Window Aggregation
windowed_df = (
    parsed_df.withWatermark("timestamp", "5 minutes")
    .groupBy(
        window("timestamp", "5 minutes", "5 minutes"), 
        "category", 
        "payment_method"
    )
    .agg(
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_quantity"),
        sum(col("price") * col("quantity")).alias("total_amount")
    )
)

# Write to PostgreSQL (transaction_details table)
def write_to_transaction_details(df, epoch_id):
    try:
        df.show(truncate=False)
        
        df.write.format("jdbc").mode("append") \
            .option("url", postgres_url) \
            .option("dbtable", "transaction_detail") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .save()
    except Exception as e:
        print(f"Error writing to PostgreSQL (transaction_detail): {e}")

# Write to PostgreSQL using foreachBatch (retail table)
def write_to_postgres(df, epoch_id):
    try:
        df.show(truncate=False)
        window_spec = Window.partitionBy("category", "payment_method").orderBy("window.start")

        df_with_columns = df.withColumn("timestamp", current_timestamp()) \
                            .withColumn("running_total", sum("total_amount").over(window_spec)) \
                            .drop("window") 

        df_with_columns.write.format("jdbc").mode("append") \
            .option("url", postgres_url) \
            .option("dbtable", "retail") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .save()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        
# Write to PostgreSQL (transaction_details)
transaction_details_df = parsed_df.select(
    "transaction_id", "customer_id", "category", "brand", "product_name", 
    "quantity", "price", "payment_method", "timestamp"
)

transaction_details_query = transaction_details_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_transaction_details) \
    .trigger(processingTime="5 minutes") \
    .start()

# Write to PostgreSQL (retail)
windowed_query = windowed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="5 minutes") \
    .start()

# Wait for termination
transaction_details_query.awaitTermination()
windowed_query.awaitTermination()