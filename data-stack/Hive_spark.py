#!/usr/bin/env python
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import time

spark = SparkSession \
    .builder \
    .appName("ExtractEventsJob") \
    .enableHiveSupport() \
    .getOrCreate()

#Default
extracted_default_events = spark.read.parquet('/tmp/default')
extracted_default_events = extracted_default_events.rdd.map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))).toDF()
extracted_default_events.registerTempTable("extracted_default_events")
if spark._jsparkSession.catalog().tableExists("default", "default"):
    query = """
            insert overwrite table default
            select * from extracted_default_events
        """
else:
    query = """
            create external table default
            stored as parquet
            location '/tmp/hive_default'
            as
            select * from extracted_default_events
        """
spark.sql(query)

#Purchase a Sword
extracted_purchase_a_sword_events = spark.read.parquet('/tmp/purchase_a_sword')
extracted_purchase_a_sword_events = extracted_purchase_a_sword_events.rdd.map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))).toDF()
extracted_purchase_a_sword_events.registerTempTable("extracted_purchase_a_sword_events")
if spark._jsparkSession.catalog().tableExists("default", "purchase_a_sword"):
    query = """
            insert overwrite table purchase_a_sword
            select * from extracted_purchase_a_sword_events
        """
else:
    query = """
            create external table purchase_a_sword
            stored as parquet
            location '/tmp/hive_purchase_a_sword'
            as
            select * from extracted_purchase_a_sword_events
        """
spark.sql(query)


#Purchase Health
extracted_purchase_health_events = spark.read.parquet('/tmp/purchase_health')
extracted_purchase_health_events = extracted_purchase_health_events.rdd.map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))).toDF()
extracted_purchase_health_events.registerTempTable("extracted_purchase_health_events")
if spark._jsparkSession.catalog().tableExists("default", "purchase_health"):
    query = """
            insert overwrite table purchase_health
            select * from extracted_purchase_health_events
        """
else:
    query = """
            create external table purchase_health
            stored as parquet
            location '/tmp/hive_purchase_health'
            as
            select * from extracted_purchase_health_events
        """
spark.sql(query)

#Join Guild
extracted_join_guild_events = spark.read.parquet('/tmp/join_guild')
extracted_join_guild_events = extracted_join_guild_events.rdd.map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw_event))).toDF()
extracted_join_guild_events.registerTempTable("extracted_join_guild_events")
if spark._jsparkSession.catalog().tableExists("default", "join_guild"):
    query = """
            insert overwrite table join_guild
            select * from extracted_join_guild_events
        """
else:
    query = """
            create external table join_guild
            stored as parquet
            location '/tmp/hive_join_guild'
            as
            select * from extracted_join_guild_events
        """
spark.sql(query)

