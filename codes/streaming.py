from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple
from pyspark.sql.utils import AnalysisException

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars", "/home/ec2-user/postgresql-42.2.28.jre7.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Kafka configuration
kafka_broker = "localhost:9092"  # Update with your Kafka broker's address
topic_name = "news_topic"  # Replace with your Kafka topic name

# Step 3: PostgreSQL connection properties
db_properties = {
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver"
}
db_url = "jdbc:postgresql://database-1.cxi0qsosqf8p.ap-south-1.rds.amazonaws.com:5432/task1"

# Step 4: Read streaming data from Kafka
def read_from_kafka():
    try:
        kafkaStreamDF = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()
        return kafkaStreamDF
    except AnalysisException as e:
        print(f"Error reading from Kafka: {e}")
        return None

kafkaStreamDF = read_from_kafka()

if kafkaStreamDF is None:
    print("Exiting due to Kafka read error.")
    exit(1)

# DEBUG: Display raw Kafka data in the console
kafkaStreamDF.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Step 5: Extract the JSON value and parse it
articlesDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json")

# DEBUG: Add this step to inspect JSON structure
articlesDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

processedDF = articlesDF.select(
    json_tuple(col("json"), "title", "description", "url").alias("title", "description", "url")
)

# DEBUG: Display processed data in the console
processedDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# Step 6: Define a function to write micro-batches to PostgreSQL
def write_to_postgresql(df, epoch_id):
    try:
        df.write \
            .jdbc(url=db_url, table="news_table", mode="append", properties=db_properties)
        print(f"Batch {epoch_id} written successfully to PostgreSQL.")
    except Exception as e:
        print(f"Error writing batch {epoch_id} to PostgreSQL: {e}")
        df.show(truncate=False)  # Show the batch data that's causing the error

# Step 7: Write the streaming data to PostgreSQL
def start_streaming():
    try:
        query = processedDF.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_postgresql) \
            .start()
        query.awaitTermination()
    except Exception as e:
        print(f"Error in streaming process: {e}")

start_streaming()

