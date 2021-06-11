#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import time

# Define the schemas
def default_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    """

    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
    ])    

def purchase_sword_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- id: integer (nullable = true)
    |-- style: string (nullable = true)
    |-- Damage: integer (nullable = true)
    |-- Speed: integer (nullable = true)
    |-- Price: integer (nullable = true)
    |-- success: boolean (nullable = true)
    """

    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("style", StringType(), True),
        StructField("Damage", IntegerType(), True),
        StructField("Speed", IntegerType(), True),
        StructField("Price", IntegerType(), True),
        StructField("success", BooleanType(), True)
    ])

def purchase_health_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- id: integer (nullable = true)
    |-- size: string (nullable = true)
    |-- health: integer (nullable = true)
    |-- Price: integer (nullable = true)
    |-- success: boolean (nullable = true)
    """

    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("size", StringType(), True),
        StructField("health", IntegerType(), True),
        StructField("Price", IntegerType(), True),
        StructField("success", BooleanType(), True)
    ])

def join_guild_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- id: integer (nullable = true)
    |-- guild_name: string (nullable = true)
    |-- success: boolean (nullable = true)
    """

    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("guild_name", StringType(), True),
        StructField("success", BooleanType(), True)
    ])

@udf('boolean')
def is_default(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'default':
        return True
    return False

@udf('boolean')
def is_purchase_a_sword(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_a_sword':
        return True
    return False

@udf('boolean')
def is_purchase_health(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_health':
        return True
    return False

@udf('boolean')
def is_join_guild(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_guild':
        return True
    return False

def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract and filter events
    extracted_default_events = raw_events \
        .filter(is_default(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          default_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    extracted_purchase_a_sword_events = raw_events \
        .filter(is_purchase_a_sword(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_sword_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*') 
    
    extracted_purchase_health_events = raw_events \
        .filter(is_purchase_health(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_health_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    extracted_join_guild_events = raw_events \
        .filter(is_join_guild(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          join_guild_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')  

    # Table into HDFS   
    sink_default = extracted_default_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_default") \
        .option("path", "/tmp/default") \
        .trigger(processingTime="3 seconds") \
        .start()

    sink_purchase_a_sword = extracted_purchase_a_sword_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchase_a_sword") \
        .option("path", "/tmp/purchase_a_sword") \
        .trigger(processingTime="3 seconds") \
        .start()

    sink_purchase_health = extracted_purchase_health_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchase_health") \
        .option("path", "/tmp/purchase_health") \
        .trigger(processingTime="3 seconds") \
        .start()
    
    sink_join_guild = extracted_join_guild_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_join_guild") \
        .option("path", "/tmp/join_guild") \
        .trigger(processingTime="3 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()