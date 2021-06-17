package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class CountDemo
 * @Date 2021/6/2 0002 14:15
 */
object CountDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CountDemo"))

    // count返回rdd元素个数
    val count: Long = sc.makeRDD(1 to 10).count()
    println(count)

    sc.stop()
  }

}
