

import java.sql.DriverManager
import java.io.ByteArrayInputStream

object pg_bytes {
  
  def main(args: Array[String]): Unit = {
    
    Class.forName("oracle.jdbc.driver.OracleDriver");
		
		val url = "jdbc:oracle:thin:@192.168.4.131:1521:orcl";
		
		val conn = DriverManager.getConnection(url, "scott", "tiger");
		
    val sql = "insert into mypbf values(?)"
    
    val pstmt = conn.prepareStatement(sql)
    
    val is = new ByteArrayInputStream(new Array[Byte](2000000))
    
    pstmt.setBlob(1, is)
    
    pstmt.execute()
    
    
  }
  
}