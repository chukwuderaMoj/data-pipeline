#!/bin/bash
echo "Launching write_hive_tables.py"
docker-compose exec spark spark-submit /w205/project-3-pacomiguelagv/write_hive_tables.py
