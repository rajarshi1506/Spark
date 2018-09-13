package poc.spark

object TimePass {
  def main(args: Array[String]) :Unit = {
    var startTime = System.currentTimeMillis()
    var rdd = Driver.getContext("local").textFile("*.*", 10)
    Enrich.wordCountGreaterThanInput(rdd, ",", 100000).foreach(println);
    println("partition count:::" + rdd.getNumPartitions)
    println("time taken::" + (System.currentTimeMillis() - startTime))
  }
}