package poc.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Driver {
  def getContext(envType: String): SparkContext = {
    var sc: SparkContext = null
    if (envType.equalsIgnoreCase("Local")) {

      sc = new SparkContext("local[2]","TimePass",new SparkConf)
    } else {

      sc = new SparkContext()
    }
    sc
  }
}