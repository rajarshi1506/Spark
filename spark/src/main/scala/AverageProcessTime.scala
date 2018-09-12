/**
 * This class receives a stream of messages in the format <process id> <time in millisecs> <status>
 * The process calculates an average of the completion times.
 */

import org.apache.spark.SparkConf
import org.apache.zookeeper.txn.SetMaxChildrenTxn
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import scala.concurrent.duration.Duration
import java.time.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import java.util.List
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.DStreamGraph

object AverageProcessTime {
  def main(args: Array[String]): Unit = {
    val checkPointDir = "tmp"

    //required to run in local file system
    System.setProperty("hadoop.home.dir", "C:\\work\\winutils")

    //create spark config
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SPARK")
    //create streaming context from checkpoint if present or create a new one
    val ssc = StreamingContext.getOrCreate(checkPointDir, () => createSparkContext(sparkConf, checkPointDir))

    //remove the debug spark logs--used instead of log4j.properties
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("listening")

    //start streaming context
    ssc.start

    ssc.awaitTermination
  }

  /**
   * create new spark context
   */
  def createSparkContext(sparkConf: SparkConf, checkPointDir: String): StreamingContext = {
    println("new spark context created")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //set checkpoint directory
    ssc.checkpoint(checkPointDir)

    //connect to socket to read the messages
    val lines = ssc.socketTextStream("10.47.134.72", 1234) //only works with nc -l -p <port> from unix server
    // lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey(updateTotalCount).foreachRDD(_.take(100).foreach(println))

    val dStream = lines.flatMap(x => {
      //split line with space
      val a = x.split(" ")
      var result: Seq[Tuple2[String, Tuple2[Int, String]]] = null
      //confirm the format is correct
      if (a.length == 3)
        result = Seq((a(0), (a(1).toInt, a(2))))
      result
    })
      //update state 
      .reduceByKey((currentSate, newState) => getLatestState(currentSate, newState))
      //update state in checkpoint
      .updateStateByKey(updateState)
      
      dStream.checkpoint(Minutes(1))

    //get average time for all completed processes
    dStream.filter((x => x._2._2.equalsIgnoreCase("STOP"))).map(x => x._2._1).repartition(1).foreachRDD(rdd => println(s"average:::${rdd.sum() / rdd.count()}"))

    //print 100 message
    dStream.foreachRDD(_.take(100).foreach(println))
    ssc
  }
  /**
   * get the latest state
   */
  private def getLatestState(currentSate: Tuple2[Int, String], newState: Tuple2[Int, String]): Tuple2[Int, String] = {
    var resultState: Tuple2[Int, String] = currentSate
    //first message for a process
    if (currentSate._2 == null && !newState._2.equalsIgnoreCase("START")) resultState = null
    else {
      //update the start time for a new process
      if (!currentSate._2.equalsIgnoreCase("STOP") && newState._2.equalsIgnoreCase("START")) resultState = (newState._1, newState._2)
      //update the stop time
      else if (newState._2.equalsIgnoreCase("STOP") && currentSate._2.equalsIgnoreCase("START")) resultState = (newState._1 - currentSate._1, newState._2)
    }
    resultState
  }

  private def updateTotalCount(newValues: Seq[Int], runningSum: Option[Integer]): Option[Integer] = {
    var result: Option[Integer] = null
    if (newValues.isEmpty) result = Some(runningSum.get)
    else {
      newValues.foreach(x => {
        if (runningSum.isEmpty) result = Some(x)
        else result = Some(x + runningSum.get)
      })
    }
    result
  }
  /**
   * update state in checkpoint
   */
  private def updateState(newValues: Seq[Tuple2[Int, String]], currentState: Option[Tuple2[Int, String]]): Option[Tuple2[Int, String]] = {
    var newState: Option[Tuple2[Int, String]] = null
    //if nothing new for key, keep current state
    if (newValues.isEmpty) newState = Some(currentState.get)
    else {
      //set the new state
      newValues.foreach(x => {
        if (currentState.isEmpty) newState = Some(x)
        else newState = Some(getLatestState(currentState.get, x))
      })
    }
    newState
  }
}
