package com.lilei.test

object App8 {

  def exec(v: Int): Set[Int] = {

    if (v < 0) exec(-v)
    else if (v < 10) Set(v)
    else exec(v / 10) + v % 10

  }
  
  def exec2(v:Int) :Set[Int] = {
    
    v match {
      case x if (x<0) => exec2 (-x)
      case y if (y<10) => Set(y)
      case _ => exec2(v / 10) + v % 10
    }
    
    
  }
  
  
  def sum(lst:List[Int]) :Int = {
    lst match {
      case Nil => 0
      case x::y => x + sum(y)
    }
    
  }
  
  

  def main(args: Array[String]): Unit = {

    println(sum(List(1,2,3)))
    
    List(1,2,3).sum
  }

}