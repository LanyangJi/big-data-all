package cn.spark.sql.c01_source

import cn.spark.sql.c01_source.beans.User
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * rdd转df
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class RddToDFDemo
 * @Date 2021/6/4 0004 9:28
 */
object RddToDFDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("RddToDFDemo")
      .getOrCreate()
    import sparkSession.implicits._

    val rdd: RDD[(String, Int)] = sparkSession.sparkContext.makeRDD(List(("张三", 23), ("李四", 33), ("王五", 77), ("赵六", 55)))
    // rdd -> dataFrame，指定列名的方式
    val inputDf: DataFrame = rdd.toDF("name", "age")
    inputDf.show()

    // 实际开发中，使用样例类的方式
    val inputDf2: DataFrame = rdd.map((ele: (String, Int)) => User(ele._1, ele._2)).toDF
    inputDf2.show()

    // dataFrame -> rdd
    val rdd2: RDD[Row] = inputDf2.rdd
    val array: Array[Row] = rdd2.collect()
    println(array.mkString(","))
    println(array(0).schema) // Row
    println(array(0)) // Row
    println(array(0).getAs[String]("name"))

    sparkSession.stop()
  }
}
