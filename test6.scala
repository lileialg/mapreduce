

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import java.util.Date

object test6 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dsfsdf").setMaster("local")
      

    val a: Array[Byte] = "dd".getBytes

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.4.128:9000/lilei/wc4")
    
    println("********************************" + new Date())
    println("=====>"+rdd1.count)
    
    println("********************************" + new Date())
    val result = rdd1.mapPartitions(f=>{
      
   
      
      Array(f.size).iterator
    })
    println("********************************" + new Date())
    
    result.collect().foreach(println)
    println("********************************" + new Date())
    rdd1.cache()
    
    
    println("********************************" + new Date())
    println("********************************")
  }
}