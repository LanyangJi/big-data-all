package cn.spark.core.c09_accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @Author jilanyang
 * @Package cn.spark.core.c09_accumulator
 * @Class CustomAccumulatorDemo
 * @Date 2021/6/2 0002 17:16
 */
object CustomAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomAccumulatorDemo")
    val sc = new SparkContext(sparkConf)

    val dataRdd: RDD[String] = sc.textFile("input/word.txt")
      .flatMap(_.split(" "))

    // 自定义累加器的使用
    val wordCountAccumulator: WordCountAccumulator = new WordCountAccumulator
    // 注册累加器
    sc.register(wordCountAccumulator)
    dataRdd.foreach(wordCountAccumulator.add)

    println(wordCountAccumulator.value)

    sc.stop()
  }

  // 自定义累加器
  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private val map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    // 累加器是否为初始状态
    override def isZero: Boolean = {
      map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      val accumulator = new WordCountAccumulator
      accumulator.map ++= this.map
      accumulator
    }

    // 重置累加器
    override def reset(): Unit = {
      map.clear()
    }

    // 向累加器中增加数据
    // 查询 map 中是否存在相同的单词
    // 如果有相同的单词，那么单词的数量加 1
    // 如果没有相同的单词，那么在 map 中增加这个单词
    override def add(v: String): Unit = {
      // 模式匹配的写法
      //      this.map.get(v) match {
      //        case None => this.map += v -> 1L
      //        case Some(n) => this.map += v -> (n + 1)
      //      }

      // 简单写法
      val newCount = this.map.getOrElse(v, 0L) + 1L
      this.map.update(v, newCount)
    }

    // 合并累加器 -> merge是在driver端，有driver完成
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      other match {
        case o: WordCountAccumulator => {
          for ((k, v) <- o.value) {
            // 模式匹配的写法
            //            this.map.get(k) match {
            //              case None => this.map += k -> v
            //              case Some(x) => this.map += k -> (v + x)
            //            }

            // 普通简化写法
            val newCount = this.map.getOrElse(k, 0L) + v
            this.map.update(k, newCount)
          }
        }
      }
    }

    // 返回累加器的结果
    override def value: mutable.Map[String, Long] = map
  }
}
