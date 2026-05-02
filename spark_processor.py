from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def write_to_postgres(batch_df, batch_id):
    """
    Processes each micro-batch of data and saves it to the PostgreSQL database.
    """
    if not batch_df.isEmpty():
        # Extract the start and end times from the window object
        final_df = batch_df.select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "symbol",
            "average_price"
        )
        
        # Write the data into Postgres using the Docker service name 'postgres'
        final_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/stocks") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "stock_averages") \
            .option("user", "user") \
            .option("password", "password") \
            .mode("append") \
            .save()
            
        print(f"Batch {batch_id} successfully written to PostgreSQL.")

# 1. Initialize Spark Session with both Kafka and PostgreSQL connectors
spark = SparkSession.builder \
    .appName("StockMovingAverage") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Reduce logging verbosity so you can see your print statements clearly
spark.sparkContext.setLogLevel("WARN")

# 2. Define the schema to match the JSON coming from producer.py
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", DoubleType())
])

print("Starting Spark Stream Processing...")

# 3. Read the stream from Kafka using the Docker service name 'kafka'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_prices") \
    .load()

# 4. Parse the JSON data and cast the timestamp
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# 5. Calculate the Moving Average 
# Using a 1-minute window, sliding every 30 seconds
windowed_avg = processed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("symbol")) \
    .agg(avg("price").alias("average_price"))

# 6. Write the stream to PostgreSQL using the foreachBatch function
query = windowed_avg.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/app/checkpoint_dir") \
    .start()

query.awaitTermination()