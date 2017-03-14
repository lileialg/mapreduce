

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put

case class POI2(id: Int, wkt: String) {

  def json: String = {

    val ab = scala.collection.mutable.ArrayBuffer[String]()

    ab += "{\"id\":" + id

    ab += "\"wkt\":\"" + wkt + "\"}"

    ab.mkString(",")

  }

}

object csv2pbf {

  //安装瓦片编号拆分数据行
  def split_row(x: (String, String)): Array[(String, String)] = {

    val splits = x._1.split("\\-")

    val results = for (r <- splits) yield (r, x._2)

    results

  }

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
      .load("hdfs://192.168.4.128:9000/lilei/poi10")

    //转对象

    val objs = df.map { x => POI2(x(0).toString().toInt, x(1).toString().split(";")(1)) }

    //生成tile_id
    val rdd2 = objs.map { x => (com.mercator.TileUtils.getTileIds(x.wkt, 15), x) }

    //转出json
    val rdd3 = rdd2.map { x => (x._1, x._2.json) }

    //进行拆分
    val rdd4 = rdd3.flatMap { x => split_row(x) }

    //进行聚合
    val rdd5 = rdd4.reduceByKey(_ + "^" + _)

    //组装protobuf
    val rdd6 = rdd5.map(x => (x._1, com.vector.tile.EncoderUtil.encodeSpark(x._2, x._1, "poi")))

    //存储hbase
    rdd6.mapPartitions(f => {

      val conf = HBaseConfiguration.create()

      conf.set("hbase.zookeeper.quorum", "Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")

      conf.set("hbase.zookeeper.property.clientPort", "2181")

      val tab = new HTable(conf, "pbf")

      import scala.collection.mutable.ListBuffer
      val puts = new ListBuffer[Put]
      for (x <- f) {

        val put = new Put(x._1.getBytes)
        put.addColumn("poi".getBytes, "value".getBytes, x._2)
        puts += put
      }

      import scala.collection.JavaConverters._

      tab.put(puts.asJava)

      Array(0).iterator
    }).collect()

  }

}