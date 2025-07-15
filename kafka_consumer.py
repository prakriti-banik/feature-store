# Notes:
# This consumer reads from user-events, computes basic features (e.g., total_purchases), and logs them.
# Extend it to store features in Redis (redis-py) and PostgreSQL (psycopg2).
import json
from confluent_kafka import Consumer, KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'feature-store-consumer',
    'auto.offset.reset': 'earliest'
}
TOPIC = 'user-events'

def compute_features(event):
    """Compute features from event (placeholder)."""
    user_id = event['user_id']
    event_type = event['event_type']
    amount = event.get('amount', 0)
    # Example: Increment purchase count if event is a purchase
    if event_type == 'purchase':
        return {'user_id': user_id, 'total_purchases': 1, 'total_amount': amount}
    return None

def main():
    try:
        consumer = Consumer(kafka_config)
        consumer.subscribe([TOPIC])
        logger.info("Kafka consumer initialized")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                break

            event = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received event: {event}")
            features = compute_features(event)
            if features:
                logger.info(f"Computed features: {features}")
                # TODO: Store features in Redis/PostgreSQL
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()