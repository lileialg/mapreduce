



import scala.io.Source
import org.apache.hadoop.io.compress.BZip2Codec
object test_url {
  
  
  def main(args: Array[String]): Unit = {
    
    val url = "http://192.168.4.128:50070/logs/yarn-root-resourcemanager-Master.Hadoop.log"
    
    val result = Source.fromURL(url,"utf-8").mkString
    
    println(result)
    
  }
}