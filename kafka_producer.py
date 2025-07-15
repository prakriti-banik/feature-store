# Setup:

# Install Kafka (Confluent Community Edition).
# Start Zookeeper and Kafka: zookeeper-server-start.sh config/zookeeper.properties, kafka-server-start.sh config/server.properties.
# Create topic: kafka-topics.sh --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1.
# Install dependency: pip install confluent-kafka.
# Run: python kafka_producer.py.

import json
import time
import random
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'feature-store-producer'
}
TOPIC = 'user-events'

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [partition {msg.partition()}]')

def generate_user_event():
    return {
        'user_id': random.randint(1, 1000),
        'event_type': random.choice(['purchase', 'click', 'view']),
        'amount': round(random.uniform(10.0, 100.0), 2) if random.choice([True, False]) else None,
        'timestamp': int(time.time())
    }

def main():
    try:
        producer = Producer(kafka_config)
        logger.info("Kafka producer initialized")
        for _ in range(100):
            event = generate_user_event()
            event_json = json.dumps(event)
            producer.produce(
                topic=TOPIC,
                value=event_json.encode('utf-8'),
                callback=delivery_report
            )
            logger.info(f"Sent event: {event_json}")
            producer.poll(0)
            time.sleep(random.uniform(0.1, 1.0))
        producer.flush()
        logger.info("All messages sent and delivered")
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    finally:
        logger.info("Producer finished")

if __name__ == "__main__":
    main()