

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test10 {
 
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("sconf").setMaster("local")
    
    
    val sc = new SparkContext(conf)
    
//    val poi = sc.textFile("hdfs://192.168.4.128:9000/lilei/core-map/poi.csv")
//    
//    poi.take(10).foreach { x => println(x) }
    
    
    
  
}
}