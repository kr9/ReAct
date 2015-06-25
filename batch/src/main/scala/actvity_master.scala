import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._
import java.util._
import com.datastax.spark.connector._
import com.datastax.driver.core.utils._
import org.apache.spark.api.java.StorageLevels._

object activity_master {
  def main(args: Array[String]) {
    val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/*.dat"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tempact1 = sqlContext.load(filepath,"json")
    tempact1.registerTempTable("activity")
    tempact1.persist(MEMORY_AND_DISK)

    // Save users
    val users = sqlContext.sql("select user_id, name, zip, lat, lon from activity GROUP BY user_id, name, zip, lat, lon")
    users.printSchema()
    case class User(user_id: String, name: String, zip: String, lat: Double, lon: Double)
    val write_user = users.map(u => User(u(0).toString, u(1).toString, u(2).toString, u(3).toString.toDouble, u(4).toString.toDouble))   
    write_user.saveToCassandra("activitydb", "user")

    // Save activities
    val activity_by_user = sqlContext.sql("SELECT user_id, zip, activity_type, activity_group_id, max(time)-min(time) as duration, lat, lon " +
            "FROM activity " +
            "GROUP BY zip, user_id, activity_type, activity_group_id, lat, lon " +
            "ORDER BY user_id")
    activity_by_user.printSchema()
    case class Activity(zip: String, activity_type: String, user_id: String, duration: Integer, lat: Double, lon: Double)
    val write_activity = activity_by_user.map(a => Activity(a(1).toString, a(2).toString, a(0).toString, a(4).toString.toDouble.toInt, a(5).toString.toDouble, a(6).toString.toDouble))   
    write_activity.saveToCassandra("activitydb", "activity_by_user")
  }
}