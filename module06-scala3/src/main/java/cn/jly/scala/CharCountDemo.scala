package cn.jly.scala

import scala.collection.mutable

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class CharCountDemo
 * @Date 2021/5/30 0030 15:42
 */
object CharCountDemo {
  def main(args: Array[String]): Unit = {
    val str = "AAAAAAAAAAAABBBBBBBBBBBBBBCXXXXXXXXXXXXXSSSSSSSSSSS"

    import scala.collection.mutable
    val charToInt = str.foldLeft(mutable.Map[Char, Int]())((map, ch) =>
      map += ch -> (map.getOrElse(ch, 0) + 1)
    )
    println(charToInt)


    val words = List("scala scala spark spark spark", "tom tom flink")
    val wordCount = words.flatMap(_.split(" ")).foldLeft(mutable.Map[String, Int]())((map, word) =>
      map += word -> (map.getOrElse(word, 0) + 1)
    )
    println(wordCount)

    val value = (1 to 100).view.filter(x => x % 2 == 0)
    println(value)

    Test(20d) match {
      case Test(n) => println(n)
    }
  }
}

case class Test(money: Double)

sealed class Test2

