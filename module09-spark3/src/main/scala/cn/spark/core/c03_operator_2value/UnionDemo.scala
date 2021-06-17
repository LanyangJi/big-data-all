package cn.spark.core.c03_operator_2value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 求rdd并集，类型必须一致
 *
 * @Author jilanyang
 * @Package c03_tansform_2value
 * @Class UnionDemo
 * @Date 2021/6/2 0002 9:33
 */
object UnionDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("UnionDemo"))
    val firstRdd: RDD[Int] = sc.makeRDD(List(1, 2))
    val secondRdd: RDD[Int] = sc.makeRDD(List(2, 3))

    val unionRdd: RDD[Int] = firstRdd.union(secondRdd)
    unionRdd.collect().foreach(println)

    sc.stop()
  }

}
