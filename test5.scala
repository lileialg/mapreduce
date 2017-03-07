

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import redis.clients.jedis.Jedis
import java.util.Date

object test5 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      

    val a: Array[Byte] = "dd".getBytes

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.4.128:9000/lilei/wc4")
    
    
    println("=====>"+rdd1.count)
    
    
    val result = rdd1.mapPartitions(f=>{
      
      
    
    val jedis = new Jedis("192.168.4.106",6379,30000)
    
    jedis.auth("lilei")
    
    jedis.select(2)
    
    val pj = jedis.pipelined()
    
    var num = 0
    
    
    
     for(v<-f) {pj.set(new Date().getTime.toString +"_"+num, v);num += 1}
    
     pj.sync()
     
     pj.close()
      
      Array(f.size).iterator
    })
    
    
    result.collect().foreach(println)
    sc.stop()
  }
}