

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
case class POI(id: Int, wkt: String, name1: String, no1: String, code: String, level: String, no2: String, no3: String, name2: String, name_en: String)

case class Person(name: String)

object poi2poi2json {

  def main(args: Array[String]): Unit = {
    //    /lilei/core-map/poi.csv

    val conf = new SparkConf()
      .setAppName("poi2poi class")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val poi_txt = sc.textFile("hdfs://192.168.4.128:9000/lilei/core-map/poi.csv").toDF()

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .load("hdfs://192.168.4.128:9000/lilei/core-map/poi.csv")

    //    [3,srid=4326;POINT (112.87057 22.892),商业设施、商务服务,null,200103,B1,802,,文化大楼,Culture Building]
    //[4,srid=4326;POINT (112.87228 22.89237),金融、保险,null,150200,B1,802,,中国太平洋财产保险股份有限公司高明支公司,China Pacific Property Insurance Co.,Ltd. Gaoming Branch]
    //[5,srid=4326;POINT (116.82451 39.75793),公司企业,null,220100,B3,1422,,鸿鑫五联股份,Hong Xin Wu Lian Stock]

    //    val pois2 = df.map(row => POI(row(0).toString.toInt, row(1).toString().split(";")(1), 
    //        row(2).toString(), row(3).toString(), row(4).toString(), row(5).toString(), 
    //        row(6).toString(), row(7).toString(), row(8).toString(), row(9).toString())).toDF()

    sc.textFile("").map { x => Person(x) }.toDF()

    val pois = df.map(row => Row(row(0).toString.toInt, row(1).toString().split(";")(1), row(2).toString(), row(3).toString(), row(4).toString(), row(5).toString(), row(6).toString(), row(7).toString(), row(8).toString(), row(9).toString()))

    import org.apache.spark.sql.types._;
    val schemaString = "customer_id name city state zip_code"

    val schema = StructType(Array(StructField("id", IntegerType, true),
      StructField("wkt", StringType, true),
      StructField("name1", StringType, true),
      StructField("no1", StringType, true),
      StructField("code", StringType, true),
      StructField("level", StringType, true),
      StructField("no2", StringType, true),
      StructField("no3", StringType, true),
      StructField("name2", StringType, true),
      StructField("name_en", StringType, true)))

    val df2 = sqlContext.createDataFrame(pois, schema)

    df2.toJSON.take(10).foreach { println }

  }

}