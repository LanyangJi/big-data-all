package cn.spark.core.c05_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class SaveDemo
 * @Date 2021/6/2 0002 14:36
 */
object SaveDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SaveDemo"))

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    rdd.saveAsTextFile("e:/rdd1")
    rdd.saveAsObjectFile("e:/rdd2")

    // saveAsSequenceFile方法要求数据必须是键值对类型
    rdd.map((_, 1)).saveAsSequenceFile("e:/rdd3")

    sc.stop()
  }

}
