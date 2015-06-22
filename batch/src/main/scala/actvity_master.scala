import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import com.datastax.driver.core.utils._
import java.util._


object activity_master
{
  def main(args: Array[String]) 
  {
    // -- Initialization --
    val host = "127.0.0.1"
    //val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/hdfs_messages_20150621025416.dat"
    //val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/*.dat"
    val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/hdfs_messages_20150621034455.dat"
    
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", host)
    val sc = new SparkContext(conf)

    // load JSON files and save as table
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //val tempact1 = sqlContext.load(filepath,"json")
    val tempact1 = sqlContext.jsonFile(filepath)
    //tempact1.show()
    tempact1.printSchema()

    tempact1.registerTempTable("activity")

    // Insert test data into Activity test table

    val list = sqlContext.sql("SELECT * FROM activity")
    // println("////////////////////////////////////////Total activities: " + list.count)
    // list.take(10).foreach(println)

    // Map DataFrame to an RDD of case classes
    
    case class write_to_activity_master(activity_id: java.util.UUID, 
        user_id: String,
        name: String,
        time: Long,
        activity_type: String, 
        lat: Double, 
        lon: Double, 
        zip: String,
        city: String)
    
    println("case class created////////////////////////////////////////")

    val write_to_Activity_Master = list.map(s => write_to_activity_master(UUIDs.random(),
        s(6).toString,
        s(4).toString,
        s(5).toString.toLong,
        s(0).toString, 
        s(2).toString.toDouble, 
        s(3).toString.toDouble, 
        s(6).toString,
        s(7).toString ))

    println("////////////////////////////////////////Total activities mapped: " + write_to_Activity_Master.count)
    // Write RDD of case classes into the Cassandra table
    write_to_Activity_Master.saveToCassandra("activitydb", "activity_master")

    println("////////////////////////////////////////Inserted into Cassandra")

    }
}