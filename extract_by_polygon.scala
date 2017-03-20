

import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

//case class GeomRow(id: String)

object extract_by_polygon {

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
      .load("hdfs://192.168.4.128:9000/lilei/green_face.csv").take(12)

    val wkt = df.map { x => x(x.length - 1).toString() }

    val face = new WKTReader().read(wkt(3).split(";")(1))
    
//    val face = new WKTReader().read("POLYGON ((80 42,82 42,82 44,80 44,80 42))")

//    println(face)

    val road = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load("hdfs://192.168.4.128:9000/lilei/road.csv")
      
    road.printSchema()   
      
    val source = road.registerTempTable("tmp_source")  

    
    
    val road1 = road.map { x => (x(0).toString(), x(x.length - 1).toString().split(";")(1)) }.
    filter(x => face.intersects(new WKTReader().read(x._2)))
    .map(x=>GeomRow(x._1)).toDF().registerTempTable("tmp_result")

    
//    val result = sqlContext.sql("select a.* from tmp_source as a left join tmp_result b on a.C0 = b.id")
    
    val result = sqlContext.sql("select a.* from  tmp_result as a")
    
    println("----->",result.count())
  }

}