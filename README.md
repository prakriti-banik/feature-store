# feature-store
Distributed Real-Time Feature Store for Machine Learning

# Docker commands for setting up and testing Kafka

## Prerequisite:
Install Homebrew: on terminal

```<bash>
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)
```
Verify: brew --version (e.g., Homebrew 4.3.x)

Java: Kafka requires Java (JDK 11+)
```bash
brew install openjdk@17
```
brew install openjdk@17

Python:
```brew install python```
Verify: python3 --version (e.g., Python 3.12.x)

Install confluent-kafka library: (using virtual environment)
```python3 -m pip install confluent-kafka```

or if using virtual environment:
```source ~/feature-store/venv/bin/activate```
```pip install confluent-kafka```

## start service
```docker-compose up -d```

## Verify Containers
```docker ps```

Expected: Shows zookeeper and kafka containers running, with ports 2181 and 9092.

## Access Kafka Container
```docker exec -it kafka bash```    

## Create user-events Topic
```docker exec -it kafka kafka-topics --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1```

## Verify topic:
```docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092```

Expected: Lists user-events.

## Test Kafka with Producer Script
Ensure confluent-kafka is Installed.
Run Producer Script:
```python3 ~/feature-store/kafka_producer.py```

## Verify Messages
```docker exec -it kafka kafka-console-consumer --topic user-events --bootstrap-server localhost:9092 --from-beginning```

Expected: Shows events like {"user_id": 123, "event_type": "purchase", ...}.

## Stop Services
```docker-compose down```

## Logging:
Kafka logs: ~/confluent/logs (manual) or Docker volumes.
Check for errors: grep ERROR ~/confluent/logs/*.

## Monitoring:
log producer/consumer metrics (e.g., delivery latency) using Python’s logging.
Optional: Add Prometheus (Docker-based) for advanced metrics.
