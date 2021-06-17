package cn.spark.sql.c01_source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class GlobalTempViewDemo
 * @Date 2021/6/2 0002 22:36
 */
object GlobalTempViewDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("GlobalTempViewDemo")
      .getOrCreate()

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    // 注册全局临时表
    inputDf.createOrReplaceGlobalTempView("user")
    // sql查询，全局临时表需要添加 global_temp前缀
    sparkSession.sql("select avg(age) as avg_age from global_temp.user").show()

    sparkSession.stop()
  }
}
