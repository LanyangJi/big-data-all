package cn.spark_advanced.core.c01_quickstart

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author jilanyang
 * @date 2021/9/14 20:59
 */
object D01_WorldCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\JLY\\hadoop2.6_Win_x64")

    val sparkConf: SparkConf = new SparkConf().setAppName("D01_WorldCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 设置日志级别
    sc.setLogLevel("INFO")

    val res: Array[(String, Int)] = sc.textFile("input/word.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()

    println(res.mkString(","))

    sc.stop()
  }
}
