package com.lilei.test

import scala.collection.mutable.ArrayBuffer

object App1 {
  
  def main(args: Array[String]): Unit = {
    
    val ab : ArrayBuffer[Int] = scala.collection.mutable.ArrayBuffer()
    
//    ab += 33
//    ab += 33
//    ab += 33
//    
//    ab.+=(333)
//    
//    ab.-=(33)
//    
//    ab.foreach(println)
    
    
//   val as =  scala.collection.mutable.ArrayStack[Int]()
//   
//   as.+=(1)
//   
//   as.+=(2)
//   
//   as.foreach { x => println }
    
    val map = scala.collection.immutable.Map(1->2,3->2)
    
    for((x,y)<- map) println(x,y)
    
  }
  
}