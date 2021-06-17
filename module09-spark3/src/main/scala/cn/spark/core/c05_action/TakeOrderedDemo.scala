package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class FirstDemo
 * @Date 2021/6/2 0002 14:17
 */
object TakeOrderedDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FirstDemo"))

    // 返回该 RDD 排序后的前 n 个元素组成的数组
    val res: Array[Int] = sc.makeRDD(List(4, 2, 1, 11, 17)).takeOrdered(3)
    println(res.mkString(","))

    sc.stop()
  }

}
