package cn.spark.sql.c01_source

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 创建临时表
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class D02_TempViewDemo
 * @Date 2021/6/2 0002 22:31
 */
object D02_TempViewDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder()
      .config(new SparkConf())
      .master("local[*]")
      .appName("D02_TempViewDemo")
      .getOrCreate()

    // 从json文件中读取
    val inputDf: DataFrame = sparkSession.read.json("input/user.json")

    // 创建临时视图，临时表. 注意：普通临时表是 Session 范围内的，如果想应用范围内有效，可以使用全局临时表。
    // 使用全局临时表时需要全路径访问，如：global_temp.people
    inputDf.createOrReplaceTempView("user")
    // 执行sql
    val dataFrame: DataFrame = sparkSession.sql("select avg(age) as avg_age from user")
    // 打印结果
    dataFrame.show()


    sparkSession.close()
  }
}
