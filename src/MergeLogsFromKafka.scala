import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.HashMap
import org.apache.spark.streaming.dstream.DStream
import kafka.serializer.Decoder
import kafka.serializer.StringDecoder

object MergeLogsFromKafka {
  def main(args: Array[String]): Unit = {

    println("69.73.176.abj,2017-06-30,10:19:39,0.0,1567864.0,0001179110-17-009773,-index.htm,301.0,659.0,1.0,0.0,0.0,10.0,0.0".split(",")(0))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      //"key.deserializer" -> "StringDeserializer",
      //"value.deserializer" -> "StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "largest"
    //,"enable.auto.commit" -> "false"
    )

    val topics = Set("test")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KAFKA STREAMING")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //get the count of 1st and second columns only
    kafkaStream.map(x => x._2.split(",")(0)+","+x._2.split(",")(1)).flatMap(_.split(",")).map(x => (x, 1)).reduceByKey(_ + _).foreachRDD(_.take(100).foreach(println))

    ssc.start()

    ssc.awaitTermination()

  }
}