package com.lilei.test

import scala.io.Source

object App9 {
  
  
  def main(args: Array[String]): Unit = {
    
     
    
    
   val s =  Source.fromFile("C:\\myfile\\software\\eclipse4.2\\eclipse\\artifacts.xml","UTF-8")
   
   val in = Source.fromInputStream(System.in, "UTF-8")
   
   val it =  in.getLines()
   
   while(it.hasNext) println(it.next())
  }
  
}