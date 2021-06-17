package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
 *
 * 分清初始值参与计算的次数
 *
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class FirstDemo
 * @Date 2021/6/2 0002 14:17
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FirstDemo"))

    val res: Int = sc.makeRDD(List(1, 2, 3, 4), 4)
      .aggregate(0)(_ + _, _ + _)
    println(res)

    sc.stop()
  }

}
