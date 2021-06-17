package cn.jly.scala

import scala.util.control.Breaks.{break, breakable}

/**
 * @author jilanyang
 * @date 2021/5/27 0027 15:03
 * @packageName cn.jly.scala 
 * @className Greeting
 */
object Greeting {
  def main(args: Array[String]): Unit = {
    println("greeting from scala")

    var i: Int = 0
    breakable {
      while (i < 100) {
        i += 1
        println(i)
        if (i == 20)
          break()
      }
    }

    // error
    // val a: Int = "12.5".toInt

    def getConn(username: String, password: String = "", url: String = "", driver: String = ""): Unit = {
      println(s"$username, $password, $url, $driver")
    }

    getConn("hello")

    // 调用包对象中的方法
    sayHello()

    val list02:List[Nothing] = List()
    println(list02.isInstanceOf[List[String]])
  }

}
