package cn.spark.streaming.c03_transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Transform 允许 DStream 上执行任意的 RDD-to-RDD 函数。即使这些函数并没有在 DStream
 * 的 API 中暴露出来，通过该函数可以方便的扩展 Spark API。该函数每一批次调度一次。其实也
 * 就是对 DStream 中的 RDD 应用转换。
 *
 * 有点类似于flink的process函数，比较通用
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c03_transform
 * @Class TransformDemo
 * @Date 2021/6/8 0008 19:05
 */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.socketTextStream("linux01", 9999)
      .transform(rdd => {
        val wordStream: RDD[String] = rdd.flatMap(_.split(" "))
        val wordAndOneStream: RDD[(String, Int)] = wordStream.map((_, 1))
        val wordAndCountStream: RDD[(String, Int)] = wordAndOneStream.reduceByKey(_ + _)
        wordAndCountStream
      })
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
