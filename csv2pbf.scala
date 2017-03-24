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

case class Road(pid: Int, wkt: String, kind: Int, direct: Int, functionclass: Int, is_viaduct: Boolean, name_zh: String, name_brief: String, brief_cnt: Int) extends source {

  def json: String = {
    val map = Map("pid" -> pid, "wkt" -> wkt, "kind" -> kind, "direct" -> direct, "functionclass" -> functionclass,
      "is_viaduct" -> is_viaduct, "name_zh" -> name_zh, "name_brief" -> name_brief, "brief_cnt" -> brief_cnt)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Railway(wkt: String, kind: Int, form: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, "form" -> form,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Poi(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Restaurant(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Hotel(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Shopping(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class CarService(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Sport(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Government(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class BusinessServices(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class ResidentsService(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Waterface(wkt: String, kind: Int, form: Int,display_class:Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, "form" -> form,"display_class" -> display_class,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class CulturalHealth(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Agriculture(wkt: String, class_1: String, subclass: String,kindcode:String,level:String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass,"kindcode" -> kindcode,"level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Greenface(wkt: String, kind: Int, form: Int,display_class:Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, "form" -> form,"display_class" -> display_class,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Adminflag(wkt: String, type_1: Double, capital: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1, "capital" -> capital,
      "name_zh" -> name_zh,"zh_cnt" -> name_zh.length())

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Tollgate(wkt: String, type_1: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Province(wkt: String, admin_id:Int,type_1: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1, "admin_id" -> admin_id,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Waterline(wkt: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Adminface(wkt: String, type_1: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Landuse(wkt: String, kind: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Eleceye(wkt: String, kind: Int, speed_limit: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "speed_limit" -> speed_limit)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Building(wkt: String, kind: Int, levels: Double) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "levels" -> levels)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Buildingmore(wkt: String, kind: Int, levels: Double) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "levels" -> levels)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Adminbound(wkt: String, kind: Int, form: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "form" -> form)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Speedlimit(wkt: String, speed_value: Int, speed_flag: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "speed_value" -> speed_value, 
      "speed_flag" -> speed_flag)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Crosswalk(wkt: String, type_1: Int, attr: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1, 
      "attr" -> attr)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class GSC(wkt: String, zlevel: Int, type_1: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "zlevel" -> zlevel, 
      "type" -> type_1)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class SAPA(wkt: String, attr: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "attr" -> attr, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Kdzone(wkt: String,  name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Aoi(wkt: String,  name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Bua(wkt: String,  name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Worldcountries(wkt: String,  name_zh: String,kind:String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Worldannotation(wkt: String,  name_zh: String,kind:String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Worldwaterface(wkt: String,  name_zh: String,kind:String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Worldwaterline(wkt: String,  name_zh: String,kind:String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, 
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}


case class Worldislands(wkt: String,  name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Trafficsignal(wkt: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Coastline(wkt: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Warninginfo(wkt: String, type_code: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type_code" -> type_code)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}



case class Restriction(wkt: String, restric_info: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "restric_info" -> restric_info)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}







case class poi(id: Int, wkt: String) extends source {

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

    val out = new PrintWriter(new FileOutputStream("/var/lib/hadoop-hdfs/lilei/tmp_scripts/" + tabName))

    out.println("create '" + tabName + "','data'")
    out.println("exit")

    out.close()

    import sys.process._

    "hbase shell /var/lib/hadoop-hdfs/lilei/tmp_scripts/" + tabName !
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

        case "poi" => df.map { x => poi(x(0).toString().toInt, x(length - 1).toString().split(";")(1)) }
        // case "road" => df.map { x => road(x(0).toString().toInt, x(length - 1).toString().split(";")(1)) }
        // case "face" => df.map { x => face(x(0).toString().toLong, x(length - 1).toString().split(";")(1)) }
      }

    hbase_create_tab(appId, zooUrl)

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