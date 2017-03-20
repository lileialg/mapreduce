

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

object csv2csv {

  def main(args: Array[String]): Unit = {

    val files = args(0)
    val domains = args(1)
    val domainValues = args(2)
    val savePosition = args(3)

    val conf = new SparkConf()
      .setAppName("local lunch")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val hdfsFiles = files.split(",")

    var df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .load(hdfsFiles(0))

    for (i <- 1 until hdfsFiles.length) {

      df = df.unionAll(sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .load(hdfsFiles(i)))

    }

    val splitsDomain = domains.split(",")

    val splitsDomainValue = domainValues.split("\\^")

    for (i <- splitsDomain) df = df.filter("C" + i + "='" + splitsDomainValue(0) + "'")

    df.write.format("csv").save(savePosition)

  }

}