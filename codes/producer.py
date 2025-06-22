from kafka import KafkaProducer
import json
from task_ansel import fetch_news

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Replace with your Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON format
)

# Kafka topic to send data to
topic_name = 'news_topic'

# Fetch the news articles using the reusable function
articles = fetch_news()

if articles:
    # Send all articles to Kafka
    for article in articles:
        producer.send(topic_name, article)

    print(f"Successfully sent {len(articles)} articles to Kafka.")
else:
    print("No articles available to send to Kafka.")

# Close the Kafka producer
producer.flush()
producer.close()