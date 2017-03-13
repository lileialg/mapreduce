
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.HTable
import redis.clients.jedis.Jedis

object hbase2redis_by_spark {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
//      .setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tablename = "account"
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
  
    rdd.mapPartitions(f => {
      
      val jedis = new Jedis("192.168.4.106",6399,30000)
      
      val pipe = jedis.pipelined()
      
      for(r <- f){
       val key = r._2.getRow
       
       val value =  r._2.getValue("cf".getBytes, "name".getBytes)
       
       pipe.set(key, value)
      }
      
      pipe.sync()
      
      Array(0).iterator
      
    }).collect()
  }

}