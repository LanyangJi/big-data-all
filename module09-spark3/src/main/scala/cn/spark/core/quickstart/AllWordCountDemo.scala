package cn.spark.core.quickstart

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

/**
 * word count方法汇总
 *
 * @author Administrator
 * @date 2021/6/16 0016 10:46
 * @packageName cn.spark.core.quickstart 
 * @className AllWordCountDemo
 */
object AllWordCountDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("AllWordCountDemo"))
    val wordsRdd: RDD[String] = sc.textFile("input/1*.txt").flatMap(_.split(" "))

    // 1. groupBy方式
    println("groupBy方式")
    wordsRdd.groupBy(v => v)
      .mapValues(_.size)
      .collect()
      .foreach(println)

    println
    // 2. groupByKey
    println("groupByKey方式 - 效率不如reduceByKey")
    wordsRdd.map((_, 1))
      .groupByKey()
      .mapValues(_.size)
      .collect()
      .foreach(println)

    println
    // 3. reduceByKey方式
    println("reduceByKey方式")
    wordsRdd.map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    println
    // 4 & 5. aggregateByKey和foldByKey方式
    println("aggregateByKey和foldByKey方式")
    wordsRdd.map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _) // foldByKey(0)(_+_)
      .collect()
      .foreach(println)

    println
    // 6. combineByKey方式
    println("combineByKey方式")
    wordsRdd.map((_, 1))
      .combineByKey(
        v => v,
        (x: Int, y) => x + y,
        (x: Int, y: Int) => x + y
      )
      .collect()
      .foreach(println)

    println
    // 7. countByKey方式
    println("countByKey方式")
    val resArr: collection.Map[String, Long] = wordsRdd.map((_, 1))
      .countByKey()
    println(resArr.mkString(","))

    println
    // 8. countByValue
    println("countByValue方式")
    val countArr: collection.Map[String, Long] = wordsRdd.countByValue()
    println(countArr.mkString(","))

    println
    // 8. reduce方式
    println("reduce方式")
    val resMap: mutable.Map[String, Long] = wordsRdd.map(word => mutable.Map[String, Long]((word, 1)))
      .reduce {
        case (map1, map2) => {
          map2.foreach {
            case (word, count) => {
              val newCount = map1.getOrElse(word, 0L) + count
              map1.update(word, newCount)
            }
          }
          map1
        }
      }
    resMap.foreach(println)

    // aggregate同理

    sc.stop()
  }
}
