you need to be inside folder pacomiguelagv

for permissions in .sh files run "chmod u+x filename.sh"
chmod u+x step1_spinup.sh
chmod u+x step2_consumer.sh
chmod u+x step3_producemessages.sh
chmod u+x benchevents.sh
chmod u+x spinup_notebook.sh


## Terminal 1

docker-compose up -d
./step1_spinup.sh    or     sh step1_spinup.sh

this will: -Create topic "events" -Describe Topic "events" -Spin Up Game game_api.py

## Terminal 2

./step2_consumer.sh    or     sh step2_consumer.sh 

this will: -Launch Kafka Consumer for topic "events"

## Terminal 3

./step3_producemessages.sh 

just with the ./ with  the sh command it will not run

this will: -Run a watch command for benchevents.sh that will create a random event every 3 seconds while running.

## Terminal 4

./step4_write_hdfs_tables.sh

this will: run the write_hive_tables.py file which will spin up spark to read in stream from kafka, transform messages into defined schemas for each event and create tables for each type of event and land them into HDFS.

## Terminal 5

./step5_make_hive_files.sh

this will: run hdfs_to_hive_for_presto.sh which will activate Hive_spark.py every 10 seconds to grab parquet files, transform them into DF and send them to hive for query with presto.


## Terminal 6

./step6_launch_presto.sh 

this will: launch presto to start querying. like:
    - show tables;
    - select count(id) from join_guild;
    - select count(id) from purchase_a_sword;
    - select count(id) from purchase_health;
    - select * from join_guild;
    - select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='join_guild';




docker-compose exec cloudera hadoop fs -ls /tmp/purchases/

docker-compose exec presto presto --server presto:8080 --catalog hive --schema default

docker-compose exec mids ab -n 1 -H "Host:user1.comcast.com" http://localhost:5000/join_guild?id=1