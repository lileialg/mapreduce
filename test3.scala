

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import redis.clients.jedis.Jedis
import java.util.Date

object test3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ddd")
      .setMaster("local")

    val a: Array[Byte] = "dd".getBytes

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.4.128:9000/lilei/wc4")
    
    
    
//    rdd1.foreach { x => {
//      
//      //jedis.set(num.toString(), x)
//      
//      num += 1
//    } }
    
    
    val result = rdd1.mapPartitions(f=>{
      
      

    val jedis = new Jedis("192.168.4.106",6379,30000)
    
    jedis.auth("lilei")
    
    jedis.select(2)
    
    var num = 0
    
    
    
     for(v<-f) {jedis.set(new Date().getTime.toString, v);num += 1}
    
     
      
      Array(num+"-"+f.count { x => true }).iterator
    })
    
    result.foreach(println)
    
    sc.stop()

  }
}