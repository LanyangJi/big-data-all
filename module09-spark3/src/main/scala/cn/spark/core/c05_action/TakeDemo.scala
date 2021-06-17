package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class FirstDemo
 * @Date 2021/6/2 0002 14:17
 */
object TakeDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FirstDemo"))

    // 取前3个，不排序。takeOrdered是取排序后的
    val res: Array[Int] = sc.makeRDD(List(4, 2, 1, 11, 17)).take(3)
    println(res.mkString(","))

    sc.stop()
  }

}
