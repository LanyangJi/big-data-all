package cn.spark.sql.c03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

/**
 * 计算平均年龄有多种方法，这里演示方法二：累加器的方式
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c03_udaf
 * @Class CalculateAvgAgeDemo01
 * @Date 2021/6/4 0004 14:11
 */
object D02_CalculateAvgAgeDemo02 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CalculateAvgAgeDemo02")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // source
    val inputDf: DataFrame = sparkSession.read.json("input/user.json")

    // 声明累加器
    val avgAccumulator = new ColumnAvgAccumulator
    // 注册累加器
    sparkSession.sparkContext.register(avgAccumulator)
    // 使用累加器
    inputDf.rdd.foreachPartition(datas => {
      datas.foreach(row => {
        avgAccumulator.add(row.getAs[Long]("age"))
      })
    })

    // 输出结果
    println(avgAccumulator.value)

    sparkSession.stop()
  }

  /**
   * 自定义求平均值的累加器
   * 求平均年龄
   */
  class ColumnAvgAccumulator extends AccumulatorV2[Long, Long] {
    var sum: Long = 0L
    var count: Long = 0L

    /**
     * 是否是初始状态
     *
     * @return
     */
    override def isZero: Boolean = {
      sum == 0 && count == 0
    }


    /**
     * 复制累加器
     *
     * @return
     */
    override def copy(): AccumulatorV2[Long, Long] = {
      val accumulator = new ColumnAvgAccumulator
      accumulator.sum = this.sum
      accumulator.count = this.count
      accumulator
    }

    /**
     * 累加器重置
     */
    override def reset(): Unit = {
      this.sum = 0L
      this.count = 0L
    }

    /**
     * 累加值
     *
     * @param v
     */
    override def add(v: Long): Unit = {
      this.sum += v
      this.count += 1
    }

    /**
     * 累加器的合并
     *
     * @param other
     */
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
      other match {
        case avgAccumulator: ColumnAvgAccumulator => {
          this.sum += avgAccumulator.sum
          this.count += avgAccumulator.count
        }
        case _ =>
      }
    }

    /**
     * 累加器的值
     *
     * @return
     */
    override def value: Long = {
      this.sum / this.count
    }
  }
}
