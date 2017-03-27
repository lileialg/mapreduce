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

case class Poi(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Restaurant(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Hotel(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Shopping(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class CarService(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Sport(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Government(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class BusinessServices(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class ResidentsService(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Waterface(wkt: String, kind: Int, form: Int, display_class: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, "form" -> form, "display_class" -> display_class,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class CulturalHealth(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Agriculture(wkt: String, class_1: String, subclass: String, kindcode: String, level: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "class" -> class_1, "subclass" -> subclass, "kindcode" -> kindcode, "level" -> level,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Greenface(wkt: String, kind: Int, form: Int, display_class: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind, "form" -> form, "display_class" -> display_class,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Adminflag(wkt: String, type_1: Double, capital: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "type" -> type_1, "capital" -> capital,
      "name_zh" -> name_zh, "zh_cnt" -> name_zh.length())

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Ptline(wkt: String, line_type: Int, status: Int, name_zh: String, city: String, color: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "line_type" -> line_type, "status" -> status,
      "name_zh" -> name_zh, "city" -> city, "color" -> color)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Subwaypolygon(wkt: String, name_zh: String, city: String, color: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt,
      "name_zh" -> name_zh, "city" -> city, "color" -> color)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Ptstop(wkt: String, name_zh: String, stationtype: Int, istransfer: Int, status: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "name_zh" -> name_zh, "stationtype" -> stationtype,
      "istransfer" -> istransfer, "status" -> status)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Rticlink(wkt: String, pid: Int, kind: Int, function_class: Int, direct: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "pid" -> pid, "kind" -> kind,
      "function_class" -> function_class, "direct" -> direct)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class DynamicTraffic(wkt: String, pid: Int, kind: Int, function_class: Int, direction: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "pid" -> pid, "kind" -> kind,
      "function_class" -> function_class, "direction" -> direction)

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

case class Ptexit(wkt: String, code: String, gatetype: Int) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "code" -> code,
      "gatetype" -> gatetype)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Pointaddr(wkt: String, fullname_zh: String, roadadress_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "fullname_zh" -> fullname_zh,
      "roadadress_zh" -> roadadress_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Annotation(wkt: String, kindcode: Int, rank: Int, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kindcode" -> kindcode,
      "rank" -> rank, "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Villtown(wkt: String, kind: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Province(wkt: String, admin_id: Int, type_1: Double, name_zh: String) extends source {

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

case class Adminface(wkt: String, type_1: Double) extends source {

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

case class GSC(wkt: String, zlevel: Int, type_1: String) extends source {

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

case class Kdzone(wkt: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Aoi(wkt: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Bua(wkt: String, name_zh: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Worldcountries(wkt: String, name_zh: String, kind: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Worldannotation(wkt: String, name_zh: String, kind: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Worldwaterface(wkt: String, name_zh: String, kind: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Worldwaterline(wkt: String, name_zh: String, kind: String) extends source {

  def json: String = {
    val map = Map("wkt" -> wkt, "kind" -> kind,
      "name_zh" -> name_zh)

    scala.util.parsing.json.JSONObject(map).toString()
  }

}

case class Worldislands(wkt: String, name_zh: String) extends source {

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

        case "Road" => df.map { x =>
          Road(x(0).toString().toInt, x(1).toString(), x(4).toString().toInt, x(5).toString().toInt, x(6).toString().toInt, x(7).toString().toBoolean,
            x(15).toString(), x(16).toString(), x(17).toString().toInt)
        }

        case "Ptline" => df.map { x =>
          Ptline(x(3).toString(), x(4).toString().toInt, x(17).toString().toInt, x(19).toString(), x(20).toString(), x(21).toString())
        }

        case "Ptstop" => df.map { x =>
          Ptstop(x(1).toString(), x(2).toString(), x(5).toString().toInt, x(6).toString().toInt, x(9).toString().toInt)
        }

        case "Poi" => df.map { x =>
          Poi(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Restaurant" => df.map { x =>
          Restaurant(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Hotel" => df.map { x =>
          Hotel(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Shopping" => df.map { x =>
          Shopping(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "CarService" => df.map { x =>
          CarService(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Sport" => df.map { x =>
          Sport(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Government" => df.map { x =>
          Government(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "BusinessServices" => df.map { x =>
          BusinessServices(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "ResidentsService" => df.map { x =>
          ResidentsService(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "CulturalHealth" => df.map { x =>
          CulturalHealth(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Agriculture" => df.map { x =>
          Agriculture(x(1).toString(), x(2).toString(), x(3).toString(), x(4).toString(), x(5).toString(), x(8).toString())
        }

        case "Waterface"       => df.map { x => Waterface(x(1).toString(), x(2).toString.toInt, x(3).toString().toInt, x(4).toString().toInt, x(6).toString) }

        case "Greenface"       => df.map { x => Greenface(x(1).toString(), x(2).toString.toInt, x(3).toString().toInt, x(4).toString().toInt, x(6).toString) }

        case "Rticlink"        => df.map { x => Rticlink(x(6).toString(), x(0).toString.toInt, x(3).toString().toInt, x(4).toString().toInt, x(7).toString.toInt) }

        case "DynamicTraffic"  => df.map { x => DynamicTraffic(x(6).toString(), x(0).toString.toInt, x(3).toString().toInt, x(4).toString().toInt, x(7).toString.toInt) }

        case "Railway"         => df.map { x => Railway(x(1).toString(), x(4).toString.toInt, x(5).toString().toInt, x(6).toString) }

        case "Annotation"      => df.map { x => Annotation(x(1).toString(), x(2).toString.toInt, x(3).toString().toInt, x(6).toString) }

        case "Adminflag"       => df.map { x => Adminflag(x(1).toString(), x(3).toString.toDouble, x(4).toString().toInt, x(5).toString) }

        case "Province"        => df.map { x => Province(x(1).toString(), x(2).toString.toInt, x(3).toString().toDouble, x(4).toString) }

        case "Tollgate"        => df.map { x => Tollgate(x(1).toString(), x(2).toString.toInt, x(4).toString()) }

        case "GSC"             => df.map { x => GSC(x(1).toString(), x(2).toString.toInt, x(4).toString()) }

        case "SAPA"            => df.map { x => SAPA(x(1).toString(), x(3).toString.toInt, x(5).toString()) }

        case "Eleceye"         => df.map { x => Eleceye(x(1).toString(), x(4).toString.toInt, x(6).toString().toInt) }

        case "Adminbound"      => df.map { x => Adminbound(x(1).toString(), x(2).toString.toInt, x(4).toString().toInt) }

        case "Crosswalk"       => df.map { x => Crosswalk(x(3).toString(), x(1).toString.toInt, x(2).toString().toInt) }

        case "Speedlimit"      => df.map { x => Speedlimit(x(5).toString(), x(3).toString.toInt, x(4).toString().toInt) }

        case "Ptexit"          => df.map { x => Ptexit(x(1).toString(), x(3).toString, x(4).toString().toInt) }

        case "Trafficsignal"   => df.map { x => Trafficsignal(x(1).toString()) }

        case "Coastline"       => df.map { x => Coastline(x(1).toString()) }

        case "Waterline"       => df.map { x => Waterline(x(1).toString()) }

        case "Warninginfo"     => df.map { x => Warninginfo(x(1).toString(), x(3).toString.toInt) }

        case "Landuse"         => df.map { x => Landuse(x(1).toString(), x(2).toString.toInt) }

        case "Building"        => df.map { x => Building(x(1).toString(), x(2).toString.toInt, x(6).toString().toDouble) }

        case "Buildingmore"    => df.map { x => Buildingmore(x(1).toString(), x(2).toString.toInt, x(4).toString().toDouble) }

        case "Subwaypolygon"   => df.map { x => Subwaypolygon(x(6).toString(), x(4).toString, x(7).toString(), x(8).toString) }

        case "Worldcountries"  => df.map { x => Worldcountries(x(3).toString(), x(1).toString, x(4).toString()) }

        case "Worldannotation" => df.map { x => Worldannotation(x(3).toString(), x(1).toString, x(4).toString()) }

        case "Worldwaterface"  => df.map { x => Worldwaterface(x(3).toString(), x(1).toString, x(4).toString()) }

        case "Worldwaterline"  => df.map { x => Worldwaterline(x(3).toString(), x(1).toString, x(4).toString()) }

        case "Pointaddr"       => df.map { x => Pointaddr(x(1).toString(), x(4).toString, x(6).toString()) }

        case "Villtown"        => df.map { x => Villtown(x(5).toString(), x(2).toString, x(3).toString()) }

        case "Restriction"     => df.map { x => Restriction(x(1).toString(), x(3).toString) }

        case "Worldislands"    => df.map { x => Worldislands(x(3).toString(), x(1).toString) }

        case "Kdzone"          => df.map { x => Kdzone(x(1).toString(), x(2).toString) }

        case "Aoi"             => df.map { x => Aoi(x(1).toString(), x(2).toString) }

        case "Bua"             => df.map { x => Bua(x(1).toString(), x(2).toString) }

        case "Adminface"       => df.map { x => Adminface(x(1).toString(), x(3).toString.toDouble) }

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