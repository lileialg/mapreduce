package com.lilei.test

object App10 {
  def main(args: Array[String]): Unit = {
    
    def test(v:Int) = {
      println(v)
      Math.pow(10, v)
    }
    
    val pv = (1 to 100).view.map { x => test(x) }.force
    
    //println(pv(10))
    
  }
}