package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class FirstDemo
 * @Date 2021/6/2 0002 14:17
 */
object FirstDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FirstDemo"))

    val first: Int = sc.makeRDD(1 to 10).first()
    println(first)

    sc.stop()
  }

}
