package com.lilei.test

import java.util.Date

object App6 {
  
  def main(args: Array[String]): Unit = {
    
    println(new Date)
    
    val a = for(i<- 1 to 30000000) yield i
    println(new Date)
    for(i<-0 to 30000000-1)
      a(i)
    println(new Date)
    
    val ee:java.util.ArrayList[Int] = null
    val ee2:java.util.LinkedList[Int] = null
  }
  
}