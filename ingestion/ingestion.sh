CONS_SLEEP = ${1:-20}

hdfs dfs -rm /user/react/history/*.dat
hdfs dfs -ls /user/react/history

cd ~/ReAct/ingestion/
echo "Running kafka producer"
python kafka_producer.py &
PROD_PID=$!
sleep 1
echo "Running kafka consumer"
python kafka_hdfs_consumer.py &
echo "Waiting for consumer to finish"
CONS_PID=$!
sleep $CONS_SLEEP
hdfs dfs -ls /user/react/history
kill $CONS_PID
