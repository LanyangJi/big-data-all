package cn.spark.streaming.c02_source

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import scala.util.control.Breaks.{break, breakable}

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c02_source
 * @Class SocketTest
 * @Date 2021/6/8 0008 14:43
 */
object SocketTest extends App {
  var input: String = _

  val socket = new Socket("linux01", 9999)
  val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

  input = reader.readLine()
  breakable {
    while (input != null) {
      if ("exit".equals(input)) {
        break()
      }
      println(input)

      input = reader.readLine()
    }
  }

  reader.close()
  socket.close()
}
