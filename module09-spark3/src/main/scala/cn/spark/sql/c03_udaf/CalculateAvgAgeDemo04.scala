package cn.spark.sql.c03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * 计算平均年龄有多种方法，这里演示方法二：弱类型UDAF
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c03_udaf
 * @Class CalculateAvgAgeDemo04
 * @Date 2021/6/4 0004 15:49
 */
object CalculateAvgAgeDemo04 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CalculateAvgAgeDemo04")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    val ds: Dataset[User] = inputDf.as[User]

    // 声明聚合函数
    val myAvgUDAF = new MyAvgUDAF

    // 自定义聚合函数的使用方式一: dsl风格
    // 将聚合函数转换为查询的列
    val column: TypedColumn[User, Double] = myAvgUDAF.toColumn
    // 查询
    ds.select(column).show()

    sparkSession.stop()

  }

  // 输入数据类型
  case class User(name: String, age: Long)

  // 缓冲区数据类型（中间计算结果数据类型）
  case class AvgBuffer(var sum: Long, var count: Long)

  /**
   * 自定义UDAF，实现Aggregator抽象类，强类型
   */
  class MyAvgUDAF extends Aggregator[User, AvgBuffer, Double] {
    /**
     * 缓冲区初始化
     *
     * @return
     */
    override def zero: AvgBuffer = {
      AvgBuffer(0L, 0L)
    }

    /**
     * 合并新元素
     *
     * @param b
     * @param a
     * @return
     */
    override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
      b.sum += a.age
      b.count += 1
      b
    }

    /**
     * 缓冲区合并
     *
     * @param b1
     * @param b2
     * @return
     */
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    /**
     * 最终计算
     *
     * @param reduction
     * @return
     */
    override def finish(reduction: AvgBuffer): Double = reduction.sum.toDouble / reduction.count

    /**
     * //DataSet 默认额编解码器，用于序列化，固定写法
     * //自定义类型就是 product 自带类型根据类型选择
     *
     * @return
     */
    override def bufferEncoder: Encoder[AvgBuffer] = {
      Encoders.product
    }

    /**
     * 最终输出类型的编码器
     *
     * @return
     */
    override def outputEncoder: Encoder[Double] = {
      Encoders.scalaDouble
    }
  }
}
