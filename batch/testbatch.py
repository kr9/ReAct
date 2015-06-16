"""testbatch.py"""
from pyspark import SparkContext, SparkConf

#conf = SparkConf().setAppName("React")
#sc = SparkContext(conf=conf)
sc = SparkContext("spark://ip-172-31-23-107:7077", "React")


folder_name = "user/react/history/"
output_folder_name = "user/react/output/"
#output_file_name = "testoutput.csv"
file_name = "*.json"
hdfs = "ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000"

file = sc.textFile("hdfs://"+hdfs+"/"+folder_name+file_name)

lines = file.map(lambda line: line.split(","))
#lines = file.flatMap(lambda line: line.split(","))#\
			#.map(lambda word: (word, 1)) \
			#.reduceByKey(lambda a, b: a + b)
#print lines
lines.collect()
lines.saveAsTextFile("hdfs://"+hdfs+"/"+output_folder_name+'test')
