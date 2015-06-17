"""testbatch.py"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

from pyspark.context import SparkConf
#from pyspark_cassandra import CassandraSparkContext, saveToCassandra

host= "127.0.0.1:9042"
#or Host="kr-lm-o"
keyspace = "activitydb"
table= "activity_by_user"


#conf = SparkConf().setAppName("PySpark Cassandra")
#conf.set("spark.cassandra.connection.host", "127.0.0.1")
#sc = CassandraSparkContext(conf=conf)

#conf = SparkConf().setAppName("React")
#sc = SparkContext(conf=conf)


class activity_by_user(Model):
  activity_id = columns.Text(primary_key=True)
  activity_type = columns.Text()
  lat = columns.Float()
  lon = columns.Float()
  time = columns.Text()
  user_id = columns.Text()
  def __repr__(self):
    return '%s %s %f %f %s %s' % (self.activity_id, self.activity_type, self.lat, self.lon, self.time, self.user_id)

connection.setup(['127.0.0.1'], "activitydb")
sc = SparkContext("spark://ip-172-31-23-107:7077", "React")
sqlContext = SQLContext(sc)




"""
def run_driver(keyspace, table):
    conf = SparkConf().setAppName("PySpark Cassandra Sample Driver")
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext(conf=conf)

    # Read some data from Cassandra
    pixels = sc.cassandraTable(keyspace, table)
    print pixels.first()

    # Count unique visitors, notice that the data returned by Cassandra is
    # a dict-like, you can access partition, clustering keys as well as
    # columns by name. CQL collections: lists, sets and maps are converted
    # to proper Python data types
    visitors = pixels.map(lambda p: (p["data"]["visitor_id"],))\
                .distinct()
    print "Visitors: {:,}".format(visitors.count())

    # Insert some new pixels into the table
    pixels = (
        {
            "customer_id": "example.com",
            "url": "http://example.com/article1/",
            "hour": dt.datetime(2014, 1, 2, 1),
            "ts": dt.datetime(2014, 1, 2, 1, 8, 23),
            "pixel_id": str(uuid4()),
            "data": {"visitor_id": "xyz"}
        },
    )
    saveToCassandra(sc.parallelize(pixels), keyspace, table)
    print "Wrote new pixels to Cassandra {!r}.{!r}".format(keyspace, table)


"""








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
mappedlist=list.map(lambda x: x.ActivityID).collect()
print mappedlist
# collection=mappedlist.collect()
sync_table(activity_by_user)

for val in mappedlist:
	savelist=activity_by_user(activty_id=val, activity_type ='null', lat=0.0, lon=0.0, time='null', user_id='null' )
  savelist.save()




#saveToCassandra(list,keyspace,table,SomeColumns("activity_id"))


#list.saveToCassandra("react", "activity", SomeColumns("activityID", "userid"))


#file = sc.textFile("hdfs://"+hdfs+"/"+folder_name+file_name)

#lines = file.map(lambda line: line.split(","))
#lines = file.flatMap(lambda line: line.split(","))#\
			#.map(lambda word: (word, 1)) \
			#.reduceByKey(lambda a, b: a + b)
#print lines
#lines.collect()
#lines.saveAsTextFile("hdfs://"+hdfs+"/"+output_folder_name+'test')
#added comments


