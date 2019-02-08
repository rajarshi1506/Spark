import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.HashMap
import org.apache.spark.streaming.dstream.DStream
import kafka.serializer.Decoder
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges

object MergeLogsFromKafka {
  def main(args: Array[String]): Unit = {
    
    // Hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()


    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      //"key.deserializer" -> "StringDeserializer",
      //"value.deserializer" -> "StringDeserializer",
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "largest"
    //,"enable.auto.commit" -> "false"
    )

    val topics = Set("test")
    //val topics = Set("test","test_partitioned","test_partitioned_4")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KAFKA STREAMING")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    kafkaStream.transform { rdd =>
   offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
   rdd
 }
    kafkaStream.transform{rdd=>
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
      }
    .map(x=>x._2).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    .foreachRDD{rdd=>
      for(offset <- offsetRanges){
        println("partition::"+offset.partition+"   topic:::"+offset.topic+" from offset:::"+offset.fromOffset+" until offset::"+offset.untilOffset)
      }
      println("partitions in rdd::"+rdd.partitions.size)
      rdd.foreach(println)
    }

    ssc.start()

    ssc.awaitTermination()

  }
}