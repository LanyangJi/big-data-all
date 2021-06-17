package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 分布式遍历 RDD 中的每一个元素，调用指定函数
 *
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class ForeachDemo
 * @Date 2021/6/2 0002 14:53
 */
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SaveDemo"))

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    // 这里的foreach是在driver端内存集合的循环遍历方法
    rdd.collect().foreach(println) // 有序

    println("-------------")
    // 为了区分不同处理效果，所以将rdd的方法称作为算子。
    // rdd外部的操作都是在Driver端执行的，内部的操作是在executor端执行的

    // 这里的foreach其实是executor端内存数据的打印（分布式打印）
    rdd.foreach(println) // 乱序

    sc.stop()
  }

}
