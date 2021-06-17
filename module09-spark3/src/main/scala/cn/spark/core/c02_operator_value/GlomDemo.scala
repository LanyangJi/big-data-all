package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 *
 * 求各个分区内的最大值
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class GlomDemo
 * @Date 2021/5/31 0031 18:54
 */
object GlomDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setAppName("GlomDemo").setMaster("local[*]"))
    // 求各个分区内的最大值
    sc.makeRDD(1 to 10, 3)
      .glom()
      .map(datas => datas.max)
      .collect()
      .foreach(println(_))

    sc.stop()
  }
}
