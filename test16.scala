

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test16 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("first lunch")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val poi = sc.textFile("hdfs://192.168.4.128:9000/lilei/core-map/poi-10.csv/")
    
    val rdd1 = poi.map { x => (com.mercator.TileUtils.getTileIds(x, 15),x) }
    
    val rdd2 = rdd1.reduceByKey(_+"^"+_)
    
    val rdd3 = rdd2.map(x => (x._1,com.vector.tile.EncoderUtil.encodeSpark(x._2, x._1, "ssssss")))
    
    val result = rdd3.collect()
    
    result.foreach(f => println(f._2.length))

  }

}