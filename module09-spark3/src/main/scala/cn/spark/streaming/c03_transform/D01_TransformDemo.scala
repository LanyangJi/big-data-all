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
 * @Class D01_TransformDemo
 * @Date 2021/6/8 0008 19:05
 */
object D01_TransformDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("D01_TransformDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.socketTextStream("linux01", 9999)
      // 外侧代码是在driver端执行
      .transform(rdd => {
        // 这边外侧代码也再driver端执行，相比较transform外面区别是这边里每一个rdd批次都会执行一次
        // rdd的算子内部，比如flatMap里面就是executor端执行的
        // transform可以将底层的rdd获取到
        // 好处： 1. DStream功能的补充
        //        2. 需要代码周期性执行的时候
        print("hello~")
        rdd.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
      })
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
