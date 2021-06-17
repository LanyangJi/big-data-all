package cn.spark.core.c02_operator_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class FlatMapDemo
 * @Date 2021/5/31 0031 18:48
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FlatMapDemo"))
    sc.makeRDD(List(List(1, 3), List(4, 5)))
      .flatMap(datas => datas)
      .collect()
      .foreach(println(_))

    // 练习：对不同数据类型做扁平化处理
    val inputRdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    // 对于不同的数据类型，采用模式匹配的方式分别处理，总之是将每个元素转换成集合（多个元素）-> 扁平化操作
    val resRdd: RDD[Int] = inputRdd.flatMap {
      case list: List[Int] =>
        list
      case x: Int =>
        List(x)
    }

    resRdd.collect().foreach(println)

    sc.stop()
  }

}
