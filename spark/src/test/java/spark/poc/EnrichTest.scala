package spark.poc

import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.GivenWhenThen
import poc.spark.Enrich
import org.apache.log4j.{Logger,Level}

class EnrichTest extends FlatSpec with BeforeAndAfter {

  private var sc: SparkContext = _
  before {

    sc = new SparkContext("local[2]", "TimePass", new SparkConf)
    Logger.getRootLogger.setLevel(Level.ERROR)
  }
  after {
    if (sc != null)
      sc.stop()
  }

  "test1" should "be equal" in {
    val lines = Seq("how are you doing")
    val inputRDD = sc.parallelize(lines, 1)
    var resultArr = Enrich.wordCount(inputRDD, " ").collect()
    var expectedArr =Array(("are", 1), ("doing", 1), ("how", 1), ("you", 1))
    assert(resultArr.sameElements(expectedArr))
  }
  
    "test2" should "not be equal" in {
    val lines = Seq("how are you doing")
    val inputRDD = sc.parallelize(lines, 1)
    var resultArr = Enrich.wordCount(inputRDD, " ").collect()
    var expectedArr =Array(("are", 1), ("doing", 2), ("how", 1), ("you", 1))
    assert(resultArr.sameElements(expectedArr))
  }
}