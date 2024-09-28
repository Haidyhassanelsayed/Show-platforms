from confluent_kafka import Producer
import json
import pandas as pd


# Load data from CSV files
try:
    amazon_data = pd.read_csv('updated_amazon_prime_titles.csv')
    netflix_data = pd.read_csv('updated_netflix_titles.csv')
    disney_data = pd.read_csv('updated_disney_plus_titles.csv')
    hulu_data = pd.read_csv('updated_hulu_titles.csv')
except Exception as e:
    print(f"Error reading CSV files: {e}")
    exit(1)

# Function to produce data to Kafka
def produce_data_to_kafka(data, topic_name):
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'excel-to-kafka-producer'
    })

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        for record in data.to_dict(orient='records'):
            producer.produce(topic_name, key=str(record.get('show_id', '')), value=json.dumps(record), callback=delivery_report)
            producer.poll(0)
        
        producer.flush()
        print(f"Data successfully produced to topic '{topic_name}'")
    except Exception as e:
        print(f"Error producing data to Kafka topic '{topic_name}': {e}")

# Produce each dataset to the single Kafka topic with four partitions
for df, source in zip([amazon_data, netflix_data, disney_data, hulu_data], ['Amazon Prime', 'Netflix', 'Disney Plus', 'Hulu']):
    df['source'] = source
    produce_data_to_kafka(df, 'media_titles')
