package cn.spark.core.quickstart

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.quickstart
 * @Class WordCountDemo
 * @Date 2021/5/30 0030 19:49
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    // 如果是打包到集群上运行，就采用这个sparkConf
    //    val sparkConf: SparkConf = new SparkConf().setAppName("wordCount")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    val contentRdd: RDD[String] = sc.textFile(args(0))
    val wordRdd: RDD[String] = contentRdd.flatMap(_.split(" "))
    val wordAndOneRdd: RDD[(String, Int)] = wordRdd.map((_, 1))
    val wordAndCountRdd: RDD[(String, Int)] = wordAndOneRdd.reduceByKey(_ + _)
    val res: Array[(String, Int)] = wordAndCountRdd.collect()

    println(res.mkString(","))

    sc.stop()
  }
}
