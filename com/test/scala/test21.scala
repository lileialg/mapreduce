package com.test.scala

import scala.sys.process.stringToProcess

object test21 {
  
  def main(args: Array[String]): Unit = {
    
//    println(new Date)
//    
//    import sys.process._
//    
//    "nohup spark-submit --master yarn --num-executors 20 --executor-memory 3500m --class test20 --name testinput --jars /data/lilei/jedis-2.8.0.jar /data/lilei/ccc.jar &" !
//    
//    
//    println(new Date)
    
     println(new java.util.Date())
    import sys.process._
    
    "nohup spark-submit --master yarn --num-executors 20 --executor-memory 3500m --class test20 --name testinput --jars /data/lilei/jedis-2.8.0.jar /data/lilei/ccc.jar &" !
    
    println(new java.util.Date())
  }
  
}