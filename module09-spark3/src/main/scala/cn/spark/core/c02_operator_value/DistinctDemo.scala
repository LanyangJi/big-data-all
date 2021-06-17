package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 去重
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class DistinctDemo
 * @Date 2021/5/31 0031 19:22
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")


    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("DistinctDemo"))
    sc.textFile("input/1*.txt")
      .flatMap(_.split(" "))
      // 底层基于reduceByKey去重
      // case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
      .distinct()
      .collect()
      .foreach(println(_))

    sc.stop()
  }

}
