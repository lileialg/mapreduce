

import org.apache.commons.net.ftp.FTPClient

object ftp_write_client {

  def main(args: Array[String]): Unit = {

    val client = new FTPClient()

    client.connect("192.168.4.106", 21)

    client.login("ftptest", "123456")

    client.makeDirectory("098765432")

    client.disconnect()

  }
}