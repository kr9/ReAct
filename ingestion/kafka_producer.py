#!/usr/bin/env python
__author__ = 'kr9'
import time
import glob, os
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class kafkaProducer(object):
    
    def __init__(self, addr):
        self.client = KafkaClient(addr) # kafka host address
        self.producer = SimpleProducer(self.client)

    def readAndStream(self):

        os.chdir("data")
        for file in glob.glob("*.dat"):# load all files in folder
            with open(file, 'rU') as f:
                content = f.readlines()# Read lines from each file
            for row in content:
                self.producer.send_messages('activity_batch', row)#send message
                print row
                timestamp1 = list(time.localtime()[0:6]) # add timestamp to message 
                print timestamp1
                #time.sleep(0.01)# Opitonal time interval between messages

activityProducer = kafkaProducer("localhost:9092")
activityProducer.readAndStream()
