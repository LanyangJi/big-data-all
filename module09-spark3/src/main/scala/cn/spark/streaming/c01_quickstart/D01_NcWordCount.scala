package cn.spark.streaming.c01_quickstart

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c01_quickstart
 * @Class D01_NcWordCount
 * @Date 2021/6/8 0008 13:45
 */
object D01_NcWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    // 配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("D01_NcWordCount")
    // 初始化一个StreamingContext
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 监听端口读取一行一行的数据
    val lineStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux01", 9999)
    // 按照空格切割
    val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))

    val wordAndOneStream: DStream[(String, Int)] = wordStream.map((_, 1))

    val wordCountStream: DStream[(String, Int)] = wordAndOneStream.reduceByKey(_ + _)

    // 打印
    wordCountStream.print()

    // 由于spark Streaming的采集器是一个长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束，所以不能让main方法执行完毕
    // 启动StreamingContext，启动采集器
    streamingContext.start()
    // 等待采集器停止
    streamingContext.awaitTermination()
  }

}
