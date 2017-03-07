

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test1 {
  def main(args: Array[String]): Unit = {
    
  val conf = new SparkConf()
  //.setMaster("yarn-cluster")

  val a: Array[Byte] = "dd".getBytes
 
  
  val sc = new SparkContext(conf)
  
  val count = sc.textFile(args(0)).flatMap { x => x.split(" ") }.map(x=>(x,1)).reduceByKey(_ + _)
  

  count.saveAsTextFile(args(1))
  
    
  }
}