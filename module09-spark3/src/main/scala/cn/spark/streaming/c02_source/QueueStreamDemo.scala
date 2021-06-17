package cn.spark.streaming.c02_source

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.concurrent.TimeUnit

/**
 * 可以通过使用 ssc.queueStream(queueOfRDDs)来创建 DStream，每一个推送到
 * 这个队列中的 RDD，都会作为一个 DStream 处理。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c02_source
 * @Class QueueStreamDemo
 * @Date 2021/6/8 0008 14:06
 */
object QueueStreamDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("QueueStreamDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    // 存放rdd的队列
    import scala.collection.mutable
    val inputQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    streamingContext.queueStream(inputQueue, oneAtATime = false)
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    // 启动任务
    streamingContext.start()

    // 向队列中放数据
    for (i <- 1 to 5) {
      inputQueue += streamingContext.sparkContext.makeRDD((1 to 100), 8)
      TimeUnit.SECONDS.sleep(2)
    }

    streamingContext.awaitTermination()
  }
}
