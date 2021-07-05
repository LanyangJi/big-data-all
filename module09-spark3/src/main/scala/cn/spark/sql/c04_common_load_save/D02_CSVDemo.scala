package cn.spark.sql.c04_common_load_save

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util

/**
 * Spark SQL 可以配置 CSV 文件的列表信息，读取 CSV 文件,CSV 文件的第一行设置为数据列
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c04_common_load_save
 * @Class CSVDemo
 * @Date 2021/6/7 0007 16:46
 */
object D02_CSVDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CSVDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    // csv配置
    val optionsMap = new util.HashMap[String, String]()
    optionsMap.put("sep", ",");
    optionsMap.put("inferSchema", "true");
    optionsMap.put("header", "true")
    // 读取文件
    val inputDf: DataFrame = sparkSession.read.options(optionsMap).csv("input/user.csv")
    inputDf.createOrReplaceTempView("user")
    val sqlDf: DataFrame = sparkSession.sql("select * from user")
    sqlDf.show()

    // 写入csv文件
    sqlDf.write.options(optionsMap).csv("output/user.csv")

    sparkSession.stop()
  }
}
