

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext



object csv_read {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//    .setAppName("dsfsdf").setMaster("local")
      
    

    val a: Array[Byte] = "dd".getBytes

    val sc = new SparkContext(conf)
    
    println(sc.applicationId)
    
//    sc.makeRDD(Seq(1 to 10)).map { x => sc. }.saveAsTextFile("/lilei/appid1")

    val rdd1 = sc.textFile("hdfs://192.168.4.128:9000/lilei/jordan.csv")

    val sqlContext = new SQLContext(sc)
    
//    val df = sqlContext.load("",Map("path"->"hdfs://192.168.4.128:9000/lilei/jordan.csv","header"->"true"))
        
    
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "false") // Automatically infer data types
    .load("hdfs://192.168.4.128:9000/lilei/jordan.csv")
    
    df.map { x => x(0) }.foreach(println)
    
    sc.makeRDD(Seq(1 to 10)).map { x => conf.getAppId }.saveAsTextFile("/lilei/appid2")
  }
}