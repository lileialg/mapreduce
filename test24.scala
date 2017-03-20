

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

object test24 {

  def main(args: Array[String]): Unit = {

    
    val conf = new SparkConf()
      .setAppName("local lunch")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)


    var df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://192.168.4.128:9000/lilei/poi.csv")

  
      df.printSchema()
      
      df.filter("C0 = '10017827'").collect().foreach(println)
  
  }

}