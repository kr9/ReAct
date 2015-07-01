# ReAct
======================================

## Real activity stats

### Finding matching users who are doing similar activities in a geographical area using data from sensors/wearables

ReAct is an application to answer a simple question:

How can I find other people who are doing similar activities in a my area? 

Running, cycling or any other activities are exciting when you have a motivating team. This application finds similar people who are doing matching activities within user's area. 

####DATA PIPELINE

![MacDown Screenshot](http://d.pr/i/10UGP+)

####CLUSTER



####Data Source
* Sensors 
* Actual sampled data is used to engineer large dataset
* Engineered data is streamed real-time
* Data Format : JSON
* Specific data: User ID, User Location (Lat, Lon), User activity type, timestamp

System is designed considering real life application design scenario. End device (wearable or sensors container) generates one JSON record with applicable data every minute. Record contains user ID, current location, User activity type, and timestamp. 

#### Folder Structure
ReAct
>ingestion - Data ingestion using Kafka and storage in HDFS

>batch - Batch processing using Spark and send to Cassandra

>stream - Stream processing*

>cassandra - Cassandra schema and table generation

>web - Web interface and related files

#### Data Ingestion
Set ReAct in your home directory and use following commands or change the directory path appropriately:

##### Start the producer. It will read the files from "data" folder

python ~/ReAct/ingestion/kafka_producer.py

##### Verify the producer is running: 

/usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic activity_batch


####Data Storage

##### Start the consumer. It will store the data to HDFS in "history" folder

python ~/react/ingestion/kafka_hdfs_consumer.py

##### Verify the data is there

hdfs dfs -ls /user/react/history
hdfs dfs -cat /user/react/history/[Filename]

####Batch Processing
Change direcotry to "batch"
Run following commands:

sbt assembly

sbt package

spark-submit --class batch_activity --master spark://ip-[Your IP]:7077 --jars target/scala-2.10/batch_activity-assembly-1.0.jar target/scala-2.10/batch_activity_2.10-1.0.jar

####Real-Time Processing
Change direcotry to "stream"

Run following commands:

sbt assembly

sbt package

spark-submit --class activity_stream --master spark://ip-[Your IP]:7077 --jars target/scala-2.10/activity_stream-assembly-1.0.jar target/scala-2.10/activity_stream-assembly-1.0.jar

######*Currently stream having some issues with cassandra connector

####Serving Layer
Run CQL file from "cassandra" folder. It will create keyspace and necessary tables. 

####API
On web address add following to get the API

#####Get all the users:

/users

#####Get list of users for particular activity in a given zip code

/users/[zip]/[activity]

For example: 

/users/93645/RUNNING

#####Get stats for the matching users for a givin user, for a given activity in a given area. 

/userstats/[user ID]/[zip]/[activity]

For example: 

/userstats/00130961/93645/RUNNING

#####Get stats for a particular user

/users/[user ID]

For example:

/users/00000099













