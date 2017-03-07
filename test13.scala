

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import redis.clients.jedis.Jedis
import java.io.PrintWriter
import java.net.URL
import org.apache.commons.net.ftp.FTPClient

object test13 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//        .setAppName("232")
//        .setMaster("local")

    val sc = new SparkContext(conf)

    val appId = sc.applicationId

    var jedis = new Jedis("192.168.4.106", 6379, 30000)

    jedis.auth("lilei")

    jedis.select(2)

    jedis.set("appid-start", appId)

    jedis.close()

    val id = sc.applicationId

   
    val client = new FTPClient

    client.connect("192.168.4.106", 21)

    client.login("ftptest", "123456")

    client.makeDirectory(id)
    
    client.disconnect()

    val rdd1 = sc.makeRDD(1 to 100000000)

    //    rdd1.saveAsTextFile(args(0))

//    rdd1.mapPartitions(f => {
//
//      val url = new URL(" ftp://ftptest:123456@192.168.4.106/javaa.txt ");
//      val pw = new PrintWriter(url.openConnection().getOutputStream());
//      
//      for(x<-f) pw.println(x)
//      
//      pw.flush();
//      pw.close();
//
//      Array().iterator
//    })
    
    
    rdd1.mapPartitionsWithIndex((index,it) => {
      val url = new URL(" ftp://ftptest:123456@192.168.4.106/"+id+"/"+index);
      val pw = new PrintWriter(url.openConnection().getOutputStream());
      
      for(x<-it) pw.println(x)
      
      pw.flush();
      pw.close();
       Array(index).iterator
    }).collect()

    jedis = new Jedis("192.168.4.106", 6379, 30000)

    jedis.auth("lilei")

    jedis.select(2)

    jedis.set("appid-stop", appId)

    jedis.close()
  }
}