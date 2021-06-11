#!/bin/bash
echo "Creating topic events..."
docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
echo "Topic events description:"
docker-compose exec kafka kafka-topics --describe --topic events --zookeeper zookeeper:32181
echo "Spining Up Game game_api.py..."
docker-compose exec mids env FLASK_APP=/w205/project-3-pacomiguelagv/game_api.py flask run --host 0.0.0.0