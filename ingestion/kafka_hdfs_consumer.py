__author__ = 'kr9'
import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os

class hdfs_Consumer(object):

    def __init__(self, addr, group, topic): # Initialize consumer object with broker address, group and topic.
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic, max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_dir_path = "/user/react/history"
        self.cached_dir_path = "/user/react/cached"
        self.topic = topic
        self.group = group
        self.block_cnt = 0

    def consume_topic(self, output_dir): #Function to consume topic. Temporary output directory to store the buffer.
        timestamp = time.strftime('%Y%m%d%H%M%S') # Get current time to append to the filename.

        #Open file wrting buffer.
        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,self.topic, self.group, timestamp)
        print self.temp_file_path
        self.temp_file = open(self.temp_file_path,"w")
        one_entry = False
        while True:
            try:
            #Get messages equivalent to count
                messages=self.consumer.get_messages(count=100,block=False)
                for message in messages:
                    one_entry = True
                    self.temp_file.write(message.message.value + "\n")
                #Divide file into specific size here 1k
                if self.temp_file.tell() > 2000:
                    self.send_to_hdfs(output_dir)
                self.consumer.commit()
            except:
                # move to tail of kafka topic if consumer is referencing
                # unknown offset
                self.consumer.seek(0, 2)
        if one_entry:
             self.send_to_hdfs(output_dir,self.topic)
        self.consumer.commit()

    def send_to_hdfs(self, output_dir):
        # Sends file into HDFS, output_dir is tempoary directory to store data before transfer
        self.temp_file.close()
        timestamp = time.strftime('%Y%m%d%H%M%S') # Get current time to append to the filename.

        hadoop_file_path = "%s/%s_%s_%s.dat" % (self.hadoop_dir_path, self.group,self.topic, timestamp)
        cached_file_path = "%s/%s_%s_%s.dat" % (self.cached_dir_path, self.group,self.topic, timestamp)
        print hadoop_file_path
        print cached_file_path
        print "Sending to HDFS ...> block " +str(self.block_cnt)
        self.block_cnt += 1
        print self.block_cnt
        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -hdfs hdfs dfs -put %s %s" % (self.temp_file_path,hadoop_file_path))
        os.system("hdfs dfs -put %s %s" % (self.temp_file_path,cached_file_path))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S') # Get current time to append to the filename.

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,self.topic,self.group,timestamp)
        print self.temp_file_path
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':

    print "\nConsuming messages...>"
    cons = hdfs_Consumer(addr="localhost:9092", group="hdfs", topic="messages")
    cons.consume_topic("/home/ubuntu/react/ingestion/kafka_messages")
