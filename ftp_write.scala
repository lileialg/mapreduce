

import java.io.PrintWriter
import java.net.URL

object ftp_write {

  def main(args: Array[String]): Unit = {
    val url = new URL(" ftp://ftptest:123456@192.168.4.106/abc/javaa.txt ");
    val pw = new PrintWriter(url.openConnection().getOutputStream());
    pw.write(" this is a test ");
    pw.flush();
    pw.close();
  }

}