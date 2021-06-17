package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
 *
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class ReduceDemo
 * @Date 2021/6/2 0002 14:10
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceDemo")
    val sc = new SparkContext(sparkConf)

    val sum: Int = sc.makeRDD(1 to 100).reduce(_ + _)
    println(sum)

    sc.stop()
  }
}
