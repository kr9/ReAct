import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
import com.databricks.spark.avro._

object SparkCassie {
  def main(args: Array[String]) {
    // -- Initialization --
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    // load JSON files and save as table
    val sqlContext = new SQLContext(sc)
    val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/activitydata.json"
    val tempact1 = sqlContext.jsonFile(filepath)
    tempact1.registerTempTable("activity")

    val list = sqlContext.sql("SELECT ActivityID FROM activity WHERE TypeID=20")
    list.show()
    list.saveToCassandra("activitydb", "tempactivity", SomeColumns("activity_id"))

