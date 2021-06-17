package cn.spark.core.c05_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class CountByKeyDemo
 * @Date 2021/6/2 0002 14:34
 */
object CountByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CountByKeyDemo"))

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2,
      "b"), (3, "c"), (3, "c")))

    // countByKey也可以做wordCount
    val map: collection.Map[Int, Long] = rdd.countByKey()
    println(map)

    sc.stop()
  }

}
