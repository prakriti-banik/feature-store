import json
from confluent_kafka import Consumer, KafkaError
import psycopg2
from datetime import datetime
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'feature-store-consumer',
    'auto.offset.reset': 'earliest'
}
TOPIC = 'user-events'

def compute_feature(existing_feature, new_event):
    """Compute updated features based on new event data."""
    user_id = new_event["user_id"]
    transaction_amount = new_event["transaction_amount"]
    age = new_event["age"]
    timestamp = datetime.strptime(new_event["timestamp"], "%Y-%m-%dT%H:%M:%S")

    # Initialize feature if none exists
    if not existing_feature:
        return {
            "user_id": user_id,
            "age": age,
            "total_transactions": transaction_amount,
            "transaction_count": 1,
            "avg_transaction_amount": transaction_amount,
            "last_transaction_timestamp": timestamp
        }

    # Update existing feature
    total_transactions = existing_feature["total_transactions"] + transaction_amount
    transaction_count = existing_feature["transaction_count"] + 1
    avg_transaction_amount = total_transactions / transaction_count

    return {
        "user_id": user_id,
        "age": age,
        "total_transactions": total_transactions,
        "transaction_count": transaction_count,
        "avg_transaction_amount": avg_transaction_amount,
        "last_transaction_timestamp": timestamp
    }

def consume_and_store():
    try:
        # Initialize Kafka consumer
        consumer = Consumer(kafka_config)
        consumer.subscribe([TOPIC])
        logger.info(f"Subscribed to topic: {TOPIC}")

        # Initialize PostgreSQL connection
        conn = psycopg2.connect(
            dbname="feature_store",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Initialize Redis connection
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        logger.info("Connected to Redis")

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                break

            # Parse incoming event
            data = json.loads(msg.value().decode('utf-8'))
            user_id = data["user_id"]

            # Fetch existing feature from PostgreSQL
            cursor.execute("""
                SELECT user_id, age, total_transactions, transaction_count, avg_transaction_amount, last_transaction_timestamp
                FROM user_features WHERE user_id = %s
            """, (user_id,))
            existing_feature = cursor.fetchone()
            existing_feature_dict = (
                {
                    "user_id": existing_feature[0],
                    "age": existing_feature[1],
                    "total_transactions": existing_feature[2],
                    "transaction_count": existing_feature[3],
                    "avg_transaction_amount": existing_feature[4],
                    "last_transaction_timestamp": existing_feature[5]
                } if existing_feature else None
            )

            # Compute updated feature
            updated_feature = compute_feature(existing_feature_dict, data)

            # Store in PostgreSQL
            cursor.execute("""
                INSERT INTO user_features (user_id, age, total_transactions, transaction_count, avg_transaction_amount, last_transaction_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    age = EXCLUDED.age,
                    total_transactions = EXCLUDED.total_transactions,
                    transaction_count = EXCLUDED.transaction_count,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
                    last_transaction_timestamp = EXCLUDED.last_transaction_timestamp,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                updated_feature["user_id"],
                updated_feature["age"],
                updated_feature["total_transactions"],
                updated_feature["transaction_count"],
                updated_feature["avg_transaction_amount"],
                updated_feature["last_transaction_timestamp"]
            ))
            conn.commit()

            # Store in Redis
            redis_client.set(
                f"feature:{user_id}",
                json.dumps({
                    "user_id": updated_feature["user_id"],
                    "age": updated_feature["age"],
                    "avg_transaction_amount": updated_feature["avg_transaction_amount"],
                    "last_transaction_timestamp": updated_feature["last_transaction_timestamp"].isoformat()
                })
            )
            logger.info(f"Processed and stored: {updated_feature}")

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
        redis_client.close()
        logger.info("Consumer finished")

if __name__ == "__main__":
    consume_and_store()