package poc.spark

import org.apache.spark.rdd.RDD

object Enrich {
  
  def wordCount(dataset:RDD[String], delimiter:String) :RDD[(String,Int)] = {
    dataset.flatMap(_.split(delimiter)).map(a=>(a,1)).reduceByKey(_+_, 1).sortByKey(true, 4)
  }
  
  def wordCountGreaterThanInput(dataset:RDD[String], delimiter:String,lowerLimit:Int) : RDD[(String,Int)] = {
    wordCount(dataset,delimiter).filter(_._2 >=lowerLimit)
  }
}