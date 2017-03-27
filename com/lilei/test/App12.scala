package com.lilei.test

object App12 {

  def values(fun: (Int) => Int, low: Int, high: Int) = {

    val result = for (i <- low to high) yield (i, fun(i))

    result
  }

  def q3(v: Int): Int = {
    (1 to v).reduceLeft(_ * _)
  }

  def largestAt(fun: (Int) => Int, inputs: Seq[Int]): Int = {
    inputs.map { x => (fun(x), x) }.max._2
  }

  def adjustToPair(x: (Int, Int) => Int, y: ((Int, Int))) = x(y._1, y._2)
  
  def unless(x: Boolean)(block : => Unit){
    if (!x)
      block
  }

  def main(args: Array[String]): Unit = {

    //    values(x => x * x, -5, 5).foreach(x => print(x._1, x._2))
    //    
    //    List(1,2,3,4).reduceLeft(Math.max(_,_))

    //    println(largestAt(x => 10 * x - x * x, 1 to 10))

    //    println(adjustToPair(_ * _, (6,7)))
    //    
    //   val result=  ((1 to 10) zip (11 to 20)).map(x=> adjustToPair(_ * _, x)).sum
    //
    //   println(result)
    
//    val result =  Array("dss","dfsdss").corresponds(Array(3,4))((x,y) => x.length() == y)
//
//    println(result)
    
    unless(13==1)(println("ok"))
  }

}