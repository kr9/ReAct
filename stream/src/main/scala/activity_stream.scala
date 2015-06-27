import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
//import org.apache.spark.sql.SQLContext.createSchemaRDD

object activity_stream {
  def main(args: Array[String]) {

    val brokers = "ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9092"
    val topics = "activity_stream"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("activity_stream").set("spark.cassandra.connection.host", "127.0.0.1")
    
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc)



    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)



    // Get the lines and show results
    messages.foreachRDD { rdd =>
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        
        System.out.println("******" + rdd.map(_._2))

        val jsonrddline= rdd.map(_._2)

        System.out.println("******JSON RDDD LINEEEEEEEEEEEEE" + jsonrddline)
        val createjsonrddline= ssc.parallelize(
            """{"user_id": "00210971", "name": "Shamika Trantow DVM", "time": "1435348363", "activity_group_id": "1435348363", "activity_type": "IDLE", "lat": "36.746375", "lon": "-119.639658", "zip": "93792", "city": "Fresno"}""" ::Nil)
        
        val tempjson = sqlContext.jsonRDD(createjsonrddline)

        val Temptable= tempjson.registerTempTable("activity")


        val tempDF = rdd.map(_._2).toDF()
        System.out.println("******/// Temp DF is printing.................")
        tempDF.show()
        //val tempact1 = sqlContext.read.json(tempDF)
        //val tempact1 = sqlContext.load(tempDF,"json")
        //System.out.println("******///" + tempDF)

        
        //tempact1.registerTempTable("activity")

        
        //val Temptable= tempDF.registerTempTable("activity")

        val list = sqlContext.sql("SELECT * FROM activity")

        System.out.println("******/// LIST IS PRINTING>>>>..... Select * from activity")
        list.show()

        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // an RDD[String] storing one JSON object per string.
        // val anotherPeopleRDD = sc.parallelize(
        // """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
        // val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)



        // //val lines = rdd.map(_.length)

        // // val ticksDF = lines.map( x => {
        // //                           val tokens = x.split(";")
        // //                           Tick(tokens(0), tokens(2).toDouble, tokens(3).toInt)}).toDF()
        // // val ticks_per_source_DF = ticksDF.groupBy("source")
        // //                         .agg("price" -> "avg", "volume" -> "sum")
        // //                         .orderBy("source")

        //Temptable.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

// object activity_master {
//   def main(args: Array[String]) {
//     val filepath = "hdfs://ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9000/user/react/history/*.dat"
//     val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
//     val sc = new SparkContext(conf)
//     val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//     val tempact1 = sqlContext.load(filepath,"json")
//     tempact1.registerTempTable("activity")

//     // Save users
//     val users = sqlContext.sql("select user_id, name, zip from activity GROUP BY user_id, name, zip")
//     users.printSchema()
//     case class User(user_id: String, name: String, zip: String)
//     val write_user = users.map(u => User(u(1).toString, u(0).toString, u(2).toString))   
//     write_user.saveToCassandra("activitydb", "user")

//     // Save activities
//     val activity_by_user = sqlContext.sql("SELECT user_id, zip, activity_type, activity_group_id, max(time)-min(time) as duration " +
//             "FROM activity " +
//             "GROUP BY zip, user_id, activity_type, activity_group_id " +
//             "ORDER BY user_id")
//     activity_by_user.printSchema()
//     case class Activity(zip: String, activity_type: String, user_id: String, duration: Integer)
//     val write_activity = activity_by_user.map(a => Activity(a(1).toString, a(2).toString, a(0).toString, a(4).toString.toDouble.toInt))   
//     write_activity.saveToCassandra("activitydb", "activity_by_user")
//   }
// }


/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}