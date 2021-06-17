package cn.jly.scala

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class JavaList2ScalaList
 * @Date 2021/5/30 0030 14:47
 */
object JavaList2ScalaList {
  def main(args: Array[String]): Unit = {
    // scala与java list互转
    val buffer: mutable.ArrayBuffer[String] = ArrayBuffer("1", "2", "3")

    val list = scalaBuffer2JavaList(buffer)
    println(list)

    val buffer2 = javaList2ScalaBuffer(list)
    println(buffer2)

    val map = Map("name" -> "tom", "age" -> 12)
    println(map("name"))

    val names = List("tom", "amy")
    names.flatMap(_.toUpperCase).foreach(println(_))
  }


  def javaList2ScalaBuffer(javaList: java.util.List[String]): ArrayBuffer[String] = {
    val buffer = ArrayBuffer[String]()
    javaList.forEach(buffer.append(_))
    buffer
  }

  def scalaBuffer2JavaList(scalaBuffer: ArrayBuffer[String]): java.util.List[String] = {
    val list:java.util.List[String] = new util.ArrayList[String]()
    for (elem <- scalaBuffer) {
      list.add(elem)
    }
    list
  }
}
