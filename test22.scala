

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test22 {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("first lunch")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
//      .load("hdfs://192.168.4.128:9000/lilei/poi10")
      .load("hdfs://192.168.4.128:9000/lilei/green_face.csv")
      
      df.take(10).foreach(println)
      
//      df.map { x => x.length }.distinct().collect().foreach(println)
      
//     val ids =  df.map { x => x(0) }
//    
//    
//     val cids = ids.collect()
//     
//     for(x<-cids){
//       
//       try{
//         x.toString().toInt
//       }catch{
//         case e:Exception => println(x)
//       }
//       
//     }
  }
  
}