#!/usr/bin/env python
__author__ = 'kr9'
import time
import csv

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class kafkaProducer(object):

    
    
    def __init__(self, addr):
        self.client = KafkaClient(addr) # kafka host address
        self.producer = SimpleProducer(self.client)

    def createData(self, activityid, typeid, uuid, timestamp, loclat, loclon): # pass the fields
        
        if activityid: #checks if any more records available
            stringRow = "%s,%s,%s,%s,%s,%s" % (activityid, typeid, uuid, timestamp, loclat, loclon)#converts each row to string
            self.producer.send_messages("messages", stringRow)#send message named messages
            print(stringRow)
            timestamp1 = list(time.localtime()[0:6])
            print timestamp1

    def readAndStream(self):

        with open('data/sampledata.csv', "rU") as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in filereader:
                self.createData(row[0],row[1],row[2],row[3],row[4],row[5])
                #print row
                time.sleep(0.03)
        filereader.close()

activityProducer = kafkaProducer("localhost:9092")
activityProducer.readAndStream()
