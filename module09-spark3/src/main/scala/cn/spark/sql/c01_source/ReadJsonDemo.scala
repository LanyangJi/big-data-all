package cn.spark.sql.c01_source

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class ReadDemo
 * @Date 2021/6/2 0002 22:17
 */
object ReadJsonDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder()
      .config(new SparkConf())
      .master("local[*]")
      .appName("ReadJsonDemo")
      .getOrCreate()

    val dataFrame: DataFrame = sparkSession.read.json("input/user.json")
    val schema: StructType = dataFrame.schema
    println(schema)

    println("----")

    dataFrame.printSchema()

    println("----")

    dataFrame.show()

    sparkSession.stop()
  }

}
