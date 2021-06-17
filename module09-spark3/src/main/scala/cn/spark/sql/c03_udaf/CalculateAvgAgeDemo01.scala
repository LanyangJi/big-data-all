package cn.spark.sql.c03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 计算平均年龄有多种方法，这里演示方法一：基于rdd方式
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c03_udaf
 * @Class CalculateAvgAgeDemo01
 * @Date 2021/6/4 0004 14:11
 */
object CalculateAvgAgeDemo01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CalculateAvgAgeDemo01")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    // source
    val inputDf: DataFrame = sparkSession.read.json("input/user.json")

    // row下标从0开始
    val res: (Long, Long) = inputDf.rdd.map((row: Row) => (row.getAs[Long]("age"), 1L))
      .reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))

    val avgAge = res._1 / res._2
    println(s"avg_age = $avgAge")

    sparkSession.stop()
  }
}
