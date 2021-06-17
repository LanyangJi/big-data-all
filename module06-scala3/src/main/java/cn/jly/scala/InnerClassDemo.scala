package cn.jly.scala

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class InnerClassDemo
 * @Date 2021/5/30 0030 13:29
 */
object InnerClassDemo {
  def main(args: Array[String]): Unit = {
    val outerClass = new OuterClass
    val innerClass = new outerClass.InnerClass

    val clazz = new OuterClass.StaticInnerClass
  }
}


class OuterClass {
  class InnerClass{
  }
}

object OuterClass {
  class StaticInnerClass{}
}