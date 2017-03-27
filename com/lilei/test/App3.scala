package com.lilei.test

import java.util.Date

object App3 {

  def main(args: Array[String]): Unit = {
  
    println(new Date)
    
    for (i <- 1 to 200) {
      var a1 = new Array[Int](3000000)
      var a2 = new Array[Int](3000000)

      var a3 = a1 +: a2

    }
    
    println(new Date)
  }

}