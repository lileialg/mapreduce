

import redis.clients.jedis.Jedis

object test12 {
  
  
  def main(args: Array[String]): Unit = {
    
    val jedis = new Jedis("192.168.4.106",6379,30000)
    
    jedis.auth("lilei")
    
    jedis.select(2)
    
    jedis.set("lilei","dssfds")
    
    jedis.close()
    
    
  }
}