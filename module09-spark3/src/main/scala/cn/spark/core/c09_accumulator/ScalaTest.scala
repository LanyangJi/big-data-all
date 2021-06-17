package cn.spark.core.c09_accumulator

/**
 * @Author jilanyang
 * @Package cn.spark.core.c09_accumulator
 * @Class ScalaTest
 * @Date 2021/6/2 0002 20:47
 */
object ScalaTest {
  def main(args: Array[String]): Unit = {
    import scala.collection.mutable
    val map: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2)

    val map1: mutable.Map[String, Int] = mutable.Map()
    val map2: mutable.Map[String, Int] = map1.foldLeft(map) {
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0) + kv._2
        innerMap
      }
    }
    println(map2)

  }

}
