package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class MapDemo
 * @Date 2021/5/31 0031 17:17
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("MapDemo"))

    sc.makeRDD(List(1, 2, 3, 4))
      .map(_ * 100)
      .collect()
      .foreach(println(_))

    sc.stop()
  }

}
