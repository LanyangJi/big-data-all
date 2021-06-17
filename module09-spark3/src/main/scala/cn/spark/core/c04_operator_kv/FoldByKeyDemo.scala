package cn.spark.core.c04_operator_kv

import org.apache.spark.{SparkConf, SparkContext}

/** 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class FoldByKeyDemo
 * @Date 2021/6/2 0002 10:15
 */
object FoldByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FoldByKeyDemo"))

    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("b", 5), ("c", 6), ("c", 1)), 2)
      .foldByKey(0)(_ + _)
      .foreach(println)

    sc.stop()
  }

}
