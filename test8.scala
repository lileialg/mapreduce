

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test8 {

  def transform(value: String): Array[(Int, Array[Byte])] = {

    val codes = for (x <- value) yield x.toInt

    val data = codes.map(x => (x, value.getBytes))

    data.toArray

  }
  
    def transform2(value: String): Array[(Int, String)] = {

    val codes = for (x <- value) yield x.toInt

    val data = codes.map(x => (x, value))

    data.toArray

  }

  def main(args: Array[String]): Unit = {

    val data: Array[String] = Array("dsada", "dsfsfsfs")

    val data1 = data.flatMap { x => transform2(x) }
    
//    data1.foreach(x=>println(x._1))
    
    data1.groupBy(x=>x._1).foreach(x=>x._2.foreach(println))

  }
}