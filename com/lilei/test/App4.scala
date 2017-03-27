package com.lilei.test

object Sample {
  def sayHello(name:String): String = {
    s"Hello $name"
  }
  
  def count :Int = 0
}

class O1{
  
}

object O1{
  def apply() = new O1
}

object App4 {
  
  def main(args: Array[String]): Unit = {
    
    Sample sayHello "abc"
    Sample count
    
    val a = List(3,2,2)
    
    a size
  }
  
}