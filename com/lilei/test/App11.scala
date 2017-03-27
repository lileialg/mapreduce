package com.lilei.test

object App11 {

  def fun(pa1:Int,pa2:String,x:(Int,String) => Boolean) {

    x(pa1,pa2)

  }
  
  
  def fun2(pa1:Int,pa2:String)(x:(Int,String) => Boolean) {

    x(pa1,pa2)

  }

  def pf(x:(Int) => Boolean): Boolean = {
   
    
    true
  }
  
  
  def fun3(x:Int) = (y:Int) => x * y
  
  
  def fun4(x:Int) = (y:Int,z:String) => {println(x,y,z);true}
  

  def main(args: Array[String]): Unit = {

    val a = fun2(1,"a"){(x,y)=> println(12);true}

    //val b = fun2(1,"a")((x,y)=> println(12);true)
    
    val c = fun3(10)
    
    val d = fun4(10)
    
    d(20,"aa")

  }

}