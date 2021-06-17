package cn.spark.core.c04_operator_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class LeftJoinDemo
 * @Date 2021/6/2 0002 13:35
 */
object LeftJoinDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LeftJoinDemo")
    val sc = new SparkContext(sparkConf)

    val firstRdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val secondRdd: RDD[(Int, String)] = sc.makeRDD(List((1, "tom"), (2, "amy")))

    val value: RDD[(Int, (String, Option[String]))] = firstRdd.leftOuterJoin(secondRdd)
    value.collect()
      .foreach(println)

    sc.stop()
  }

}
