

object test7 {
  
  
  def main(args: Array[String]): Unit = {
    val data = Array(2,3,4,5).iterator
    
    println(data.mkString(","))
    
    while(data.hasNext) println(data.next())
    
    println(data.size)
    
    for(x<-data) println("---"+x)
    
    println(data.size)
  }
}