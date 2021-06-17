package cn.jly.scala

import scala.annotation.tailrec

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class ClosureTest
 * @Date 2021/5/30 0030 17:41
 */
object ClosureTest {
  def main(args: Array[String]): Unit = {

    def addSuffix(suffix: String): String => String = {
      (fileName: String) => if (fileName.endsWith(suffix)) fileName else fileName + suffix
    }

    println(addSuffix(".jpg")("hello"))
    println(addSuffix(".jpg")("hello.jpg"))

    println()

    implicit class TestEq(str: String) {
      def checkEq(targetStr: String)(f: (String, String) => Boolean): Boolean = {
        f(str.toLowerCase, targetStr.toLowerCase)
      }
    }

    println("hello".checkEq("HELLo")(_.equals(_)))

    println()

    // 实现while的效果
    @tailrec
    def myWhile(condition: => Boolean)(action: => Unit): Unit = {
      if (condition) {
        action
        myWhile(condition)(action)
      }
    }

    var i = 0
    var sum = 0
    myWhile(i <= 100) {
      sum += i
      i += 1
    }

    println(sum)
  }
}
