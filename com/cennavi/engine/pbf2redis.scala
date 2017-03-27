
package com.cennavi.engine

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

object pbf2redis {

  def main(args: Array[String]): Unit = {

    val zoo_url = args(0)

    val tabName = args(1)

    val sourceName = args(2)

    val redis_ip = args(3)

    val redis_port = args(4).toInt

    val sparkConf = new SparkConf()
    
    val sc = new SparkContext(sparkConf)
    
    val conf = HBaseConfiguration.create()
    
    conf.set("hbase.zookeeper.quorum", zoo_url)
    
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    
    conf.set(TableInputFormat.INPUT_TABLE, tabName)
    
    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    rdd.mapPartitions(f => {

      val jedis = new Jedis(redis_ip, redis_port, 30000)

      val pipe = jedis.pipelined()

      for (r <- f) {
        val key = r._2.getRow

        val value = r._2.getValue("data".getBytes, sourceName.getBytes)

        pipe.set((new String(key) + "," + sourceName).getBytes, value)
      }

      pipe.sync()

      Array(0).iterator

    }).collect()
  }

}