import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import com.datastax.spark.connector._
import scala.collection.JavaConversions._


object batch_activity
{
  def main(args: Array[String]) 
  {
    // -- Initialization --
    val host = "127.0.0.1"
    val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activityjson.json"
    
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", host)
    val sc = new SparkContext(conf)

    // load JSON files and save as table
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    
    val tempact1 = sqlContext.jsonFile(filepath)
    //val tempact1 = sqlContext.jsonFile("hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activityjson.json")

    tempact1.registerTempTable("activity")

    // Insert test data into Activity test table

    val list = sqlContext.sql("SELECT ActivityID FROM activity")
    list.collect.foreach(println)



    //list.show() // Shows list on command line

    // Map DataFrame to an RDD of case classes
    case class write_tempactivity(activity_id: String)
    val write_Tempactivity = list.map( r => write_tempactivity(r(0).toString ))



    // Write RDD of case classes into the Cassandra table
    write_Tempactivity.saveToCassandra("activitydb", "tempactivity")


    println("ActivityID sent to Cassandra")


    //Create new case class and insert data into activity_by_user table

    val alldata = sqlContext.sql("SELECT * FROM activity")
    alldata.collect.foreach(println)

    println("Data from Activity table////////////////////////////////////////")

      // Map DataFrame to an RDD of case classes
    case class write_to_master_activity(activity_id: String, lat: Double, lon: Double, time: String, activity_type: String, user_id: String)
    
    println("case class created////////////////////////////////////////")


    val write_to_Master_Activity = alldata.map( s => write_to_master_activity(s(0).toString, s(1).toString.toDouble, s(2).toString.toDouble, s(3).toString,s(4).toString, s(5).toString ))

    // Write RDD of case classes into the Cassandra table
    write_to_Master_Activity.saveToCassandra("activitydb", "master_activity")


    }
}