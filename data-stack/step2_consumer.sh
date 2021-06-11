#!/bin/bash
echo "Launching Kafka Consumer..."
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning