package cn.spark.sql.c02_udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author jilanyang
 * @Package cn.spark.sql.c02_udf
 * @Class UDFDemo
 * @Date 2021/6/4 0004 13:49
 */
object D01_UDFDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDFDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")

    // 注册udf
    sparkSession.udf.register("addPrefix", addPrefix(_,_))
    // 注册视图
    inputDf.createOrReplaceTempView("user")
    // 执行sql
    sparkSession.sql("select addPrefix(name, 'name: ') as new_name from user")
      .show()

    sparkSession.stop()
  }

  /**
   * 自定义函数
   *
   * @param column
   * @param prefix
   * @return
   */
  def addPrefix(column: String, prefix: String): String = prefix + column
}
