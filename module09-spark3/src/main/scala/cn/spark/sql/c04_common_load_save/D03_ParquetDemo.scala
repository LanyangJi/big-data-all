package cn.spark.sql.c04_common_load_save

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Spark SQL 的默认数据源为 Parquet 格式。Parquet 是一种能够有效存储嵌套数据的列式
 * 存储格式。
 * 数据源为 Parquet 文件时，Spark SQL 可以方便的执行所有的操作，不需要使用 format。
 * 修改配置项 spark.sql.sources.default，可修改默认数据源格式。
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c04_common_load_save
 * @Class ParquetDemo
 * @Date 2021/6/4 0004 17:18
 */
object D03_ParquetDemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ParquetDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    // 保存为parquet格式
    inputDf.write.format("parquet").mode(SaveMode.Append).save("output/user.parquet")

    sparkSession.stop()
  }
}
