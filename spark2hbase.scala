
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes 
import org.apache.hadoop.hbase.client.HTable
object spark2hbase {
  
  def main(args: Array[String]): Unit = {
    
   val sparkConf = new SparkConf()
   //.setAppName("HBaseTest").setMaster("local")  
    val sc = new SparkContext(sparkConf)  
      
    val tablename = "account"  
      
//    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")  
//    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")  
//    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)  
      
//    val job = new Job(sc.hadoopConfiguration)  
//    job.setOutputKeyClass(classOf[ImmutableBytesWritable])  
//    job.setOutputValueClass(classOf[Result])    
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])    
  
    val indataRDD = sc.makeRDD(1 to 9999999,2000).map(_.toString())
    indataRDD.mapPartitions(f => {
      
      val conf = HBaseConfiguration.create()
      
      conf.set("hbase.zookeeper.quorum","Master.Hadoop:2181,Slave2.Hadoop:2181,Slave1.Hadoop:2181")
      
      conf.set("hbase.zookeeper.property.clientPort", "2181")  
      
      val tab = new HTable(conf,tablename)
      
      import scala.collection.mutable.ListBuffer
      val puts = new ListBuffer[Put]
      for(x<-f){
        
//        val arr = x.split(",")
        val put = new Put(x.getBytes)
        put.addColumn("cf".getBytes, "name".getBytes, x.getBytes)
        put.addColumn("cf".getBytes, "age".getBytes, x.getBytes)
        puts += put
      }
      
     import scala.collection.JavaConverters._
     
      
      tab.put(puts.asJava)
      
      Array(0).iterator
    }).collect()

      
//    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())  
  }  
  
}