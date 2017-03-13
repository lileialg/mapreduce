

object test17 {

  case class c(id: String)
  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql._

    val conf = new SparkConf()
      .setAppName("poi2poi class")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile("").map { x => c(x) }.toDF()

  }

}