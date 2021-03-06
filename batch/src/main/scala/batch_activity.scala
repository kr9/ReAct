import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import java.util._
import org.apache.spark.api.java.StorageLevels._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.driver.core.utils._
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.GroupedData

object batch_activity {
  def main(args: Array[String]) {
    val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/*.dat"
    //Test one file
    //val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/hdfs_activity_batch_20150628052422.dat"
    // -- Initialization --
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // load JSON files and save as table
    val tempact1 = sqlContext.load(filepath,"json")
    tempact1.registerTempTable("activity")
    tempact1.persist(MEMORY_AND_DISK)

    //View content of the table
    //val list = sqlContext.sql("SELECT * FROM activity")
    //list.show()


    // Save users
    val users = sqlContext.sql("SELECT user_id, name, zip, lat, lon FROM activity GROUP BY user_id, name, zip, lat, lon")
    users.printSchema()
    // Map DataFrame to an RDD of case classes
    case class User(user_id: String, name: String, zip: String, lat: Double, lon: Double)
    val write_user = users.map(u => User(u(0).toString, u(1).toString, u(2).toString, u(3).toString.toDouble, u(4).toString.toDouble))   
    // Write RDD of case classes into the Cassandra table
    write_user.saveToCassandra("activitydb", "user")


    // Save activities
    val activity_by_user = sqlContext.sql("SELECT user_id, zip, activity_type, activity_group_id, max(time)-min(time) AS duration_for_total, max(time)-min(time) AS duration_for_avg, lat, lon " +
            "FROM activity " +
            "GROUP BY zip, user_id, activity_type, activity_group_id, lat, lon " +
            "ORDER BY user_id")
    //activity_by_user.show()
    activity_by_user.printSchema()
    import sqlContext.implicits._
    val activity_avg = activity_by_user.groupBy("user_id", "zip", "lat", "lon", "activity_type").agg(scala.collection.immutable.Map("duration_for_total" ->"sum", "duration_for_avg"->"avg"))
    //val activity_avg = activity_by_user.groupBy("user_id", "zip", "activity_type", "lat", "lon").agg($"user_id", $"zip", $"activity_type", $"lat", $"lon", sum($"duration_for_total"), avg($"duration_for_avg"))
    //activity_avg.foreach(println)
    activity_avg.printSchema()
    activity_avg.show()
    // Map DataFrame to an RDD of case classes - Activity By user 
    // case class Activity(zip: String, activity_type: String, user_id: String, duration: Integer, lat: Double, lon: Double)
    // val write_activity = activity_by_user.map(a => Activity(a(1).toString, a(2).toString, a(0).toString, a(4).toString.toDouble.toInt, a(5).toString.toDouble, a(6).toString.toDouble)) 
    // // Write RDD of case classes into the Cassandra table  
    // write_activity.saveToCassandra("activitydb", "activity_by_user")

    // Map DataFrame to an RDD of case classes - Activity Avg
    case class Activity_Avg(user_id: String, zip: String,lat: Double, lon: Double, activity_type: String, SUM: Double, AVG: Double)
    val write_activity_avg = activity_avg.map(a => Activity_Avg(a(0).toString, a(1).toString, a(2).toString.toDouble, a(3).toString.toDouble, a(4).toString, a(5).toString.toDouble, a(6).toString.toDouble)) 
    // // Write RDD of case classes into the Cassandra table  
    write_activity_avg.saveToCassandra("activitydb", "activity_avg")

  }
}