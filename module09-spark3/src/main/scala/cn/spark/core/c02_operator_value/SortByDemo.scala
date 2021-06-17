package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
 * 的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
 * 致。中间存在 shuffle 的过程
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class SortByDemo
 * @Date 2021/5/31 0031 20:01
 */
object SortByDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SortByDemo"))
    sc.makeRDD(List(6,3,2,5,4), 2)
      .sortBy(x => x, ascending = true)
      .saveAsTextFile("output/sortBy")

    sc.stop()
  }

}
