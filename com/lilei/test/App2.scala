package com.lilei.test

import java.util.Date

object App2 {

  def main(args: Array[String]): Unit = {
  
    println(new Date)
    
    for (i <- 1 to 200) {
      val a1 = new Array[Int](3000000)
      val a2 = new Array[Int](3000000)

      val a3 = a1 ++ a2

    }
    
    println(new Date)
    
  }

}