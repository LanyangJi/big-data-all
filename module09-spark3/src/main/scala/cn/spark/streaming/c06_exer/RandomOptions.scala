package cn.spark.streaming.c06_exer

import java.util.Random
import scala.collection.mutable.ListBuffer

/**
 *
 * @param value  值
 * @param weight 比重
 * @tparam T
 */
case class RandOpt[T](value: T, weight: Int)

object RandomOptions {
  def apply[T](opts: RandOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      // 根据weight比重，把对应RandOpt的值加weight次进入optsBuffer中
      for (elem <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }
}

class RandomOptions[T](opts: RandOpt[T]*) {

  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}
