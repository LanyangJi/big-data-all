package cn.spark.sql.c03_udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * 计算平均年龄有多种方法，这里演示方法三：弱类型UDAF
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c03_udaf
 * @Class CalculateAvgAgeDemo03
 * @Date 2021/6/4 0004 15:15
 */
object D03_CalculateAvgAgeDemo03 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CalculateAvgAgeDemo03")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    // 创建视图
    inputDf.createOrReplaceTempView("user")
    // 注册自定义的udaf
    sparkSession.udf.register("myAvg", new AvgUDAF)
    // 使用
    sparkSession.sql("select myAvg(age) from user").show()

    sparkSession.stop()
  }

  // 弱类型已经过期了，现在推荐强类型的函数
  class AvgUDAF extends UserDefinedAggregateFunction {
    /**
     * 输入参数的数据类型
     *
     * @return
     */
    override def inputSchema: StructType = {
      StructType(Seq(StructField("age", LongType)))
    }

    /**
     * 聚合函数缓冲区的值的类型
     *
     * @return
     */
    override def bufferSchema: StructType = {
      StructType(Seq(StructField("sum", LongType), StructField("count", LongType)))
    }

    /**
     * 函数返回值的数据类型
     *
     * @return
     */
    override def dataType: DataType = {
      DoubleType
    }

    /**
     * 稳定性：对于相同的输入是否一直返回相同的输出。
     *
     * @return
     */
    override def deterministic: Boolean = {
      true
    }

    /**
     * 函数缓冲区初始化
     *
     * @param buffer
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L

      // 另一种写法
      // buffer.update(0, 0L)
    }

    /**
     * 更新缓冲区
     *
     * @param buffer
     * @param input
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getAs[Long](0)
        buffer(1) = buffer.getLong(1) + 1L
      }
    }

    /**
     * 合并缓冲区
     *
     * @param buffer1
     * @param buffer2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    /**
     * 计算最终结果
     *
     * @param buffer
     * @return
     */
    override def evaluate(buffer: Row): Double = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }
}
