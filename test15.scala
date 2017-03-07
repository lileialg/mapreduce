

import com.mercator.TileUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object test15 {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    .setAppName("first lunch")
    .setMaster("local")
    
    
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    val poi = sc.textFile("hdfs://192.168.4.128:9000/lilei/core-map/poi.csv")
    
   val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "false") // Automatically infer data types
    .load("hdfs://192.168.4.128:9000/lilei/core-map/poi.csv")
    
    val poi10 = sc.makeRDD(df.map { x => x(1).toString().split(";")(1) }.take(10))
    
    poi10.saveAsTextFile("hdfs://192.168.4.128:9000/lilei/core-map/poi-10.csv")
  }
  
  
}