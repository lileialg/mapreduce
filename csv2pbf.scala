package com.cennavi.engine

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import com.mercator.TileUtils
import java.io.FileOutputStream
import java.io.PrintWriter

abstract class source {

  def wkt: String

  def json: String
}


case class poi(id: Int, wkt: String) extends source {

  def json: String = {

    val ab = scala.collection.mutable.ArrayBuffer[String]()

    ab += "{\"id\":" + id

    ab += "\"wkt\":\"" + wkt + "\"}"

    ab.mkString(",")

  }

}

case class road(id: Int, wkt: String) extends source {

  def json: String = {

    val ab = scala.collection.mutable.ArrayBuffer[String]()

    ab += "{\"id\":" + id

    ab += "\"wkt\":\"" + wkt + "\"}"

    ab.mkString(",")

  }

}

case class face(id: Long, wkt: String) extends source {

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

  //hbase创建表
  def hbase_create_tab(tabName: String, zooUrl: String) {
//    val conf = HBaseConfiguration.create()
//
//    //      conf.set("hbase.zookeeper.quorum", "Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")
//
//    conf.set("hbase.zookeeper.quorum", zooUrl)
//
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//
//    val connection = ConnectionFactory.createConnection(conf);
//    val admin = connection.getAdmin()
//
//    val hTableDescriptor = new HTableDescriptor(tabName);
//    val hColumnDescriptor = new HColumnDescriptor("data");
//    hTableDescriptor.addFamily(hColumnDescriptor);
//    admin.createTable(hTableDescriptor);
    
    val out = new PrintWriter(new FileOutputStream("/var/lib/hadoop-hdfs/lilei/tmp_scripts/"+tabName))
    
    out.println("create '"+tabName+"','data'")
    out.println("exit")
    
    out.close()
    
    import sys.process._
    
    "hbase shell /var/lib/hadoop-hdfs/lilei/tmp_scripts/"+tabName !
  }

  def main(args: Array[String]): Unit = {

    val sourceName = args(0)
    val position = args(1)
    val zooUrl = args(2)
    val zooms = args(3)

    val conf = new SparkConf()
    //      .setAppName("first lunch")
    //      .setMaster("local")

    val sc = new SparkContext(conf)
    
    val appId = sc.applicationId

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      //      .load("hdfs://192.168.4.128:9000/lilei/poi10")
      //      .load("hdfs://192.168.4.128:9000/lilei/green_face.csv")
      .load(position).repartition(20)

    //转对象

    val length = df.take(1)(0).length

    val objs =
      sourceName match {

        case "poi"  => df.map { x =>  poi(x(0).toString().toInt, x(length - 1).toString().split(";")(1)) }
        case "road" => df.map { x =>  road(x(0).toString().toInt, x(length - 1).toString().split(";")(1)) }
        case "face" => df.map { x =>  face(x(0).toString().toLong, x(length - 1).toString().split(";")(1)) }
      }

    hbase_create_tab(appId,zooUrl)
    
    for (z <- zooms.split(",")) {
      

      //生成tile_id
      val rdd2 = objs.map { x => (com.mercator.TileUtils.getTileIds(x.wkt, z.toInt), x) }

      //转出json
      val rdd3 = rdd2.map { x => (x._1, x._2.json) }

      //进行拆分
      val rdd4 = rdd3.flatMap { x => split_row(x) }

      //进行聚合
      val rdd5 = rdd4.reduceByKey(_ + "^" + _)

      //组装protobuf
      val rdd6 = rdd5.map(x => (x._1, com.vector.tile.EncoderUtil.encodeSpark(x._2, x._1, sourceName)))

      //存储hbase
      rdd6.mapPartitions(f => {

        val conf = HBaseConfiguration.create()

        //      conf.set("hbase.zookeeper.quorum", "Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")

        conf.set("hbase.zookeeper.quorum", zooUrl)

        conf.set("hbase.zookeeper.property.clientPort", "2181")

        val tab = new HTable(conf, appId)

        import scala.collection.mutable.ListBuffer
        val puts = new ListBuffer[Put]

        for (x <- f) {

          if (x._2.length > 0) {
            val put = new Put(x._1.getBytes)
            put.addColumn("data".getBytes, sourceName.getBytes, x._2)
            puts += put

          }

          if (puts.size >= 5000) {
            import scala.collection.JavaConverters._

            tab.put(puts.asJava)

            puts.clear()
          }
        }

        import scala.collection.JavaConverters._

        tab.put(puts.asJava)

        Array(0).iterator
      }).collect()

    }
  }

}