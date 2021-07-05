package cn.spark.sql.c03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * 计算平均年龄有多种方法，这里演示方法五：强类型UDAF -> sql风格
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c03_udaf
 * @Class CalculateAvgAgeDemo05
 * @Date 2021/6/4 0004 16:26
 */
object D05_CalculateAvgAgeDemo05 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CalculateAvgAgeDemo05")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    // 使用自定义UDAF方式二
    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    // 创建视图
    inputDf.createOrReplaceTempView("user")
    // 声明并注册自定义的UDAF
    val myAvgAgg = new MyAvgAgg
    sparkSession.udf.register("myAvgAgg", functions.udaf(myAvgAgg))
    // 执行sql
    sparkSession.sql("select myAvgAgg(age) as avg_age from user").show()

    sparkSession.stop()
  }

  // 缓冲区数据类型（中间计算类型）
  case class Buffer(var sum: Long, var count: Long)

  class MyAvgAgg extends Aggregator[Long, Buffer, Double] {
    override def zero: Buffer = Buffer(0L, 0L)

    override def reduce(b: Buffer, a: Long): Buffer = {
      b.sum += a
      b.count += 1
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Buffer): Double = reduction.sum / reduction.count

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}
