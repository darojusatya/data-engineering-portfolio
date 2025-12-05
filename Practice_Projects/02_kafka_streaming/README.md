# Kafka Real-Time Streaming Pipeline (Local Docker)
This project contains:
- docker-compose to run Zookeeper + Kafka + Schema Registry (light)
- a Python producer that pushes JSON events
- a PySpark Structured Streaming job that reads from Kafka, aggregates, and writes parquet
- instructions to run locally
