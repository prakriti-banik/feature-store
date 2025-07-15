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
        'user_id': f"user{random.randint(1, 1000)}",
        'age': random.randint(18, 80),
        'transaction_amount': round(random.uniform(10.0, 500.0), 2),
        'timestamp': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
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