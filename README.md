# Distributed Real-Time Feature Store for Machine Learning

This project implements a distributed feature store for machine learning, ingesting data via Kafka, processing it in real-time, storing features in PostgreSQL, and serving them via a FastAPI endpoint.

## Setup
1. Ensure Virtusl Environment is activated: `source ~/feature-store/venv/bin/activate` and Docker is running.
2. Start Kafka and PostgreSQL: `cd docker && docker-compose up -d`
3. Install dependencies: `pip install -r requirements.txt`
4. Ensure Topic Exists: `kafka-topics.sh --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`
5. Run the producer: `python3 producer/feature_producer.py`
6. Start the API: `python3 api/feature_api.py`
7. Run the consumer: `python3 consumer/feature_consumer.py`
8. Access features: `curl http://localhost:8000/features/user1`

## Structure
- `producer/`: Kafka producer to ingest feature data.
- `consumer/`: Kafka consumer to process and store features.
- `api/`: FastAPI server to serve features.
- `docker/`: Docker configurations for PostgreSQL.

## Test:
- Use `curl http://localhost:8000/features/user123` (replace `user123` with a generated `user_id`).
- Verify Redis caching with `redis-cli`:
    - Connect: `docker exec -it <redis-container-name> redis-cli` (redis-container-name can be found from `docker ps`)
    - Check keys: `KEYS feature:*`
    - Get a feature: `GET feature:user123`

# Docker commands for setting up and testing Kafka

## Prerequisite:
Install Homebrew: on terminal

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)
```
Verify: brew --version (e.g., Homebrew 4.3.x)

Java: Kafka requires Java (JDK 11+)
```bash
brew install openjdk@17
```
brew install openjdk@17

Python:
```bash
brew install python
```
Verify: python3 --version (e.g., Python 3.12.x)

Install confluent-kafka library: (using virtual environment)
```bash
python3 -m pip install confluent-kafka
```

or if using virtual environment:
```bash
source ~/feature-store/venv/bin/activate
pip install confluent-kafka
```

## start service
```bash
docker-compose up -d
```

## Verify Containers
```bash
docker ps
```

Expected: Shows zookeeper and kafka containers running, with ports 2181 and 9092.

## Access Kafka Container
```bash
docker exec -it kafka bash
```    

## Create user-events Topic
```bash
docker exec -it kafka kafka-topics --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Verify topic:
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected: Lists user-events.

## Test Kafka with Producer Script
Ensure confluent-kafka is Installed.
Run Producer Script:
```bash
python3 ~/feature-store/kafka_producer.py
```

## Verify Messages
```bash
docker exec -it kafka kafka-console-consumer --topic user-events --bootstrap-server localhost:9092 --from-beginning
```

Expected: Shows events like {"user_id": 123, "event_type": "purchase", ...}.

## Stop Services
```bash
docker-compose down
```

## Logging:
Kafka logs: ~/confluent/logs (manual) or Docker volumes.
Check for errors: grep ERROR ~/confluent/logs/*.

## Monitoring:
log producer/consumer metrics (e.g., delivery latency) using Python’s logging.
Optional: Add Prometheus (Docker-based) for advanced metrics.

