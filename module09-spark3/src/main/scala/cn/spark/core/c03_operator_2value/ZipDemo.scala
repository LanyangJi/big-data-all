package cn.spark.core.c03_operator_2value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将两个 RDD 中的元素，以键值对的形式进行合并。其中，键值对中的 Key 为第 1 个 RDD
 * 中的元素，Value 为第 2 个 RDD 中的相同位置的元素。
 *
 * 1. 类型可以不一致
 * 2. 分区数必须一致
 * 3. 各分区数量必须一致
 *
 * @Author jilanyang
 * @Package c03_tansform_2value
 * @Class ZipDemo
 * @Date 2021/6/2 0002 9:43
 */
object ZipDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ZipDemo"))
    val firstRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)
    val secondRdd: RDD[String] = sc.makeRDD(List("tom", "amy", "lily"), 3)

    val zipRdd: RDD[(Int, String)] = firstRdd.zip(secondRdd)
    zipRdd.foreach(println)

    sc.stop()
  }

}
