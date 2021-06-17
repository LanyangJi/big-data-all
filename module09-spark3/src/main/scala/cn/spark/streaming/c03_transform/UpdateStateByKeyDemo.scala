package cn.spark.streaming.c03_transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * UpdateStateByKey 原语用于记录历史记录，有时，我们需要在 DStream 中跨批次维护状态(例
 * 如流计算中累加 wordcount)。针对这种情况，updateStateByKey()为我们提供了对一个状态变量
 * 的访问，用于键值对形式的 DStream。给定一个由(键，事件)对构成的 DStream，并传递一个指
 * 定如何根据新的事件更新每个键对应状态的函数，它可以构建出一个新的 DStream，其内部数
 * 据为(键，状态) 对。
 * updateStateByKey() 的结果会是一个新的 DStream，其内部的 RDD 序列是由每个时间区间对
 * 应的(键，状态)对组成的。
 * updateStateByKey 操作使得我们可以在用新信息进行更新时保持任意的状态。为使用这个功
 * 能，需要做下面两步：
 * 1. 定义状态，状态可以是一个任意的数据类型。
 * 2. 定义状态更新函数，用此函数阐明如何使用之前的状态和来自输入流的新值对状态进行更
 * 新。
 * 使用 updateStateByKey 需要对检查点目录进行配置，会使用检查点来保存状态。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c03_transform
 * @Class UpdateStateByKeyDemo
 * @Date 2021/6/8 0008 19:38
 */
object UpdateStateByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UpdateStateByKeyDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 开启检查点
    streamingContext.checkpoint("spark-streaming-checkpoint")

    val wordAndOneStream: DStream[(String, Int)] = streamingContext.socketTextStream("linux01", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))

    // 使用 updateStateByKey 来更新状态，统计从运行开始以来单词总的次数
    val stateStream: DStream[(String, Int)] = wordAndOneStream.updateStateByKey[Int](updateFunc(_, _))
    stateStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
   * 定义更新状态方法，参数 values 为当前批次单词频度，state 为以往批次单词频度
   *
   * @param values 最新批次rdd数据
   * @param state  状态
   * @return
   */
  def updateFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
    val rddCount: Int = values.sum
    val previousCount: Int = state.getOrElse(0)
    Some(rddCount + previousCount)
  }

}
