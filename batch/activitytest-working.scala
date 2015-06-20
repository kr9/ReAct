import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
//import SQLContext.implicits._
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//import org.json4s.JsonDSL._
import com.datastax.spark.connector._
//import scala.collection.JavaConversions._


object activitytest 
{
  def main(args: Array[String]) 
  {
    // -- Initialization --
  
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    // load JSON files and save as table
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    //val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activitydata.json"
    //val tempact1 = sqlContext.jsonFile(filepath)
    val tempact1 = sqlContext.jsonFile("hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activityjson.json")
    //val tempact1 = sqlContext.jsonFile("hdfs://ip-172-31-23-107.us-west-2.compute.internal:9000/user/react/history/activitydata.json")

    tempact1.registerTempTable("activity")

    val list = sqlContext.sql("SELECT ActivityID FROM activity")

    //list.show()

    // Map DataFrame to an RDD of case classes
    case class write_tempactivity(activity_id: String)
    val write_Tempactivity = list.map( r => write_tempactivity(r(0).toString ))

    // Write RDD of case classes into the Cassandra table
    write_Tempactivity.saveToCassandra("activitydb", "tempactivity")

/** Example from Insight Blog
    import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}

// read Cassandra table in as an RDD composed of case classes
case class ByCountyMonth(state: String, county: String, date: String, count: Int)
val byCountyMonth = sc.cassandraTable[ByCountyMonth]('messages', 'by_county_month')

// Infer schema from RDD composed of case classes to a DataFrame
val df = byCountyMonth.toDF

// Compute average number of messages sent per month by county
val avgByCounty = df.groupBy('state','county').agg('count' -> 'avg')

// Map DataFrame to an RDD of case classes
case class ByCountyAvg(state: String, county: String, average: Double)
val byCountyAvg = avgByCounty.map( r => ByCountyAvg(r(0).toString, r(1).toString, r(2).asInstanceOf[Double]) )

// Write RDD of case classes into the Cassandra table
byCountyAvg.saveToCassandra('messages', 'by_county_avg', writeConf=WriteConf(ttl = TTLOption.constant(360)))
**/







    //list.saveToCassandra("activitydb", "tempactivity", SomeColumns("activity_id"))
    }
}