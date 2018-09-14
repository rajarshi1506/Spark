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

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "StringDeserializer",
     "value.deserializer" -> "StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "smallest"
      ,"enable.auto.commit" -> "false"
      )

    val topics = Set("test")
    
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KAFKA STREAMING")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    
    kafkaStream.flatMap(_._2.split(" ")).map(x=>(x,1)).reduceByKey(_+_).foreachRDD(_.foreach(println))
    
    ssc.start()
    
    ssc.awaitTermination()
    
  }
}