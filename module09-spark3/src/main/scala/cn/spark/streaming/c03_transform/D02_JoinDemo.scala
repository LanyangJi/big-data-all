package cn.spark.streaming.c03_transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 两个流之间的 join 需要两个流的批次大小一致，这样才能做到同时触发计算。计算过程就是
 * 对当前批次的两个流中各自的 RDD 进行 join，与两个 RDD 的 join 效果相同。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c03_transform
 * @Class D02_JoinDemo
 * @Date 2021/6/8 0008 19:14
 */
object D02_JoinDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("D02_JoinDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 创建2个流
    val inputStream1: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux01", 9999)
    val inputStream2: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux01", 8888)

    val wordAndOneStream: DStream[(String, Int)] = inputStream1.flatMap(_.split(" ")).map((_, 1))
    val wordAndAStream: DStream[(String, String)] = inputStream2.flatMap(_.split(" ")).map((_, "a"))

    // join -> 底层就是两个rdd的join (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.join(rdd2, partitioner)
    val joinStream: DStream[(String, (Int, String))] = wordAndOneStream.join(wordAndAStream)
    // 打印头10个
    joinStream.print(10)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
