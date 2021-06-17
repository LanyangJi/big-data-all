package cn.jly.scala

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class RecursiveTest
 * @Date 2021/5/30 0030 18:48
 */
object RecursiveTest {
  def main(args: Array[String]): Unit = {
    // list最大值
    def max(list: List[Int]): Int =
      if (list.isEmpty)
        throw new NoSuchElementException
      else if (list.size == 1)
        list.head
      else if (list.head > max(list.tail))
        list.head
      else
        max(list.tail)

    println(max(List(1, 11, 23, 44, 8)))

    // 字符串反转
    def reverseStr(str: String): String =
      if (str.length == 1) str else reverseStr(str.tail) + str.head

    println(reverseStr("hello"))
  }
}
