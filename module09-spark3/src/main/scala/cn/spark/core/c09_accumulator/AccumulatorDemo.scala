package cn.spark.core.c09_accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器 - 只写变量
 *
 * 累加器用来把 Executor 端变量信息聚合到 Driver 端。在 Driver 程序中定义的变量，在
 * Executor 端的每个 Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后，
 * 传回 Driver 端进行 merge。
 *
 * @Author jilanyang
 * @Package cn.spark.core.c09_accumulator
 * @Class AccumulatorDemo
 * @Date 2021/6/2 0002 17:11
 */
object AccumulatorDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")


    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("AccumulatorDemo"))
    val dataRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 声明系统累加器（spark提供了简单的数据累加器） -> 主要目的就是executor可以将计算结果回传到Driver端进行合并
    val sumAccumulator: LongAccumulator = sc.longAccumulator("sum")

    // 如果转换算子（如map）中使用了累加器，但是没有行动算子触发，累加器的值不会改变
    // 如果该转换算子被多个行动算子触发，则会导致累加器多加
    // 一般累加器放在行动算子中进行操作
    dataRdd.foreach(ele => sumAccumulator.add(ele))

    println(sumAccumulator.value)

    sc.stop()
  }

}
