package cn.spark.streaming.c03_transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Window Operations 可以设置窗口的大小和滑动窗口的间隔来动态的获取当前 Steaming 的允许
 * 状态。所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长。
 * ➢  窗口时长：计算内容的时间范围；
 * ➢  滑动步长：隔多久触发一次计算。
 * 注意：这两者都必须为采集周期大小的整数倍。
 * WordCount 第三版：3 秒一个批次，窗口 12 秒，滑步 6 秒。
 *
 * 关于 Window 的操作还有如下方法：
 * （1）window(windowLength, slideInterval): 基于对源 DStream 窗化的批次进行计算返回一个
 * 新的 Dstream；
 * （2）countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数；
 * （3）reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间
 * 流元素来创建一个新的单元素流；
 * （4）reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]): 当在一个(K,V)
 * 对的 DStream 上调用此函数，会返回一个新(K,V)对的 DStream，此处通过对滑动窗口中批次数
 * 据使用 reduce 函数来整合每个 key 的 value 值。
 * （5）reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 这个函
 * 数是上述函数的变化版本，每个窗口的 reduce 值都是通过用前一个窗的 reduce 值来递增计算。
 * 通过 reduce 进入到滑动窗口数据并”反向 reduce”离开窗口的旧数据来实现这个操作。一个例
 * 子是随着窗口滑动对 keys 的“加”“减”计数。通过前边介绍可以想到，这个函数只适用于”
 * 可逆的 reduce 函数”，也就是这些 reduce 函数有相应的”反 reduce”函数(以参数 invFunc 形式
 * 传入)。如前述函数，reduce 任务的数量通过可选参数来配置。
 * val ipDStream = accessLogsDStream.map(logEntry => (logEntry.getIpAddress(), 1))
 * val ipCountDStream = ipDStream.reduceByKeyAndWindow(
 * {(x, y) => x + y},
 * {(x, y) => x - y},
 * Seconds(30),
 * Seconds(10))
 * //加上新进入窗口的批次中的元素 //移除离开窗口的老批次中的元素 //窗口时长// 滑动步长
 * countByWindow()和 countByValueAndWindow()作为对数据进行计数操作的简写。
 * countByWindow()返回一个表示每个窗口中元素个数的 DStream，而 countByValueAndWindow()
 * 返回的 DStream 则包含窗口中每个值的个数。
 * val ipDStream = accessLogsDStream.map{entry => entry.getIpAddress()}
 * val ipAddressRequestCount = ipDStream.countByValueAndWindow(Seconds(30),
 * Seconds(10))
 * val requestCount = accessLogsDStream.countByWindow(Seconds(30), Seconds(10))
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c03_transform
 * @Class WindowOperationDemo
 * @Date 2021/6/9 0009 10:55
 */
object WindowOperationDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    // 需求：每6秒统计一下过去12秒的单词个数
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WindowOperationDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))


    val windowStream: DStream[(String, Int)] = streamingContext.socketTextStream("linux01", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(12),
        Seconds(6)
      )

    windowStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
