import json
import os
from kafka import KafkaConsumer
import boto3

# AWS S3 Configuration
AWS_REGION = ""
S3_BUCKET_NAME = ""
S3_KEY_PREFIX = ""

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv(""),
    aws_secret_access_key=os.getenv(""),
    region_name=AWS_REGION
)

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "news_topic"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='your-group-id',
    value_deserializer=lambda x: x.decode('utf-8')
)

def upload_to_s3(message, key):
    """Upload JSON message to S3."""
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(message),
            ContentType='application/json'
        )
        print(f"Uploaded {key} to S3.")
    except Exception as e:
        print(f"Upload failed: {e}")

# Listen for messages from Kafka
for message in consumer:
    try:
        article = json.loads(message.value)  # Parse message as JSON
        s3_key = f"{S3_KEY_PREFIX}article_{message.offset}.json"  # Create unique S3 key
        upload_to_s3(article, s3_key)
    except json.JSONDecodeError:
        print(f"Invalid JSON: {message.value}")
    except Exception as e:
        print(f"Error processing message: {e}")

