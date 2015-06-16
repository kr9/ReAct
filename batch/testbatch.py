"""testbatch.py"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

#conf = SparkConf().setAppName("React")
#sc = SparkContext(conf=conf)
sc = SparkContext("spark://ip-172-31-23-107:7077", "React")
sqlContext = SQLContext(sc)

folder_name = "user/react/history/"
output_folder_name = "user/react/output/"
#output_file_name = "testoutput.csv"
file_name = "*.json"
hdfs = "ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000"


#SQL Test.
path = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activitydata.json"
act = sqlContext.jsonFile(path)
#act.printSchema()
act.registerTempTable("activity")
#list = sqlContext.sql("SELECT * FROM activity")
list = sqlContext.sql("SELECT ActivityID FROM activity WHERE TypeID=20")
list.show()


#file = sc.textFile("hdfs://"+hdfs+"/"+folder_name+file_name)

#lines = file.map(lambda line: line.split(","))
#lines = file.flatMap(lambda line: line.split(","))#\
			#.map(lambda word: (word, 1)) \
			#.reduceByKey(lambda a, b: a + b)
#print lines
#lines.collect()
#lines.saveAsTextFile("hdfs://"+hdfs+"/"+output_folder_name+'test')
#added comments


