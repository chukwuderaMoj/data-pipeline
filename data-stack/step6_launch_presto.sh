#!/bin/bash
echo "Launching Presto..."
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
