package cn.spark.core.c03_operator_2value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 返回两个rdd的交集
 * 类型不一致报错
 *
 * @Author jilanyang
 * @Package c03_tansform_2value
 * @Class IntersectionDemo
 * @Date 2021/6/2 0002 9:28
 */
object IntersectionDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("IntersectionDemo"))
    val firstRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    val secondRdd: RDD[Int] = sc.makeRDD(List(2, 4))

    val intersectionRdd: RDD[Int] = firstRdd.intersection(secondRdd)
    intersectionRdd.collect().foreach(println)

    sc.stop()
  }
}
