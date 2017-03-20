

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.vividsolutions.jts.io.WKTReader

object test27 {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("local lunch")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
 
    

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://192.168.4.128:9000/lilei/road.csv")
    
    val r1 =  df.map { x => x(x.length-1).toString().split(";")(1) } .collect().
    filter { x =>  new WKTReader().read(x).toString().contains("EMPTY") }.foreach { x => println(x) }
    
    
      
  }
  
  
}