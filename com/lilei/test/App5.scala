package com.lilei.test

import java.util.Date

object App5 {
  
  def main(args: Array[String]): Unit = {
    println(new Date)
    val a = scala.collection.mutable.ListBuffer[Int]()
    
    for(i<-1 to 30000000) a += i
    println(new Date)
//    for(i<-0 to 30000000-1)
//      a(i)
    for(i<-a.iterator)
      i
    
    println(new Date)
  }
  
}