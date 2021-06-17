package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的
 * RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition
 * 操作都可以完成，因为无论如何都会经 shuffle 过程。
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class RepartitionDemo
 * @Date 2021/5/31 0031 19:55
 */
object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("RepartitionDemo"))
    sc.makeRDD(List(1, 2, 3, 4, 5), 2)
      .repartition(4)
      .mapPartitionsWithIndex((index, datas) => datas.map(("分区" + index, _)))
      .collect()
      .foreach(println(_))

    sc.stop()
  }

}
