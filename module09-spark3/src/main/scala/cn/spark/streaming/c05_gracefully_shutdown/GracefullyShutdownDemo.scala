package cn.spark.streaming.c05_gracefully_shutdown

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.net.URI
import java.util.concurrent.TimeUnit

/**
 * 流式任务需要 7*24 小时执行，但是有时涉及到升级代码需要主动停止程序，但是分
 * 布式程序，没办法做到一个个进程去杀死，所以配置优雅关机就显得至关重要了。
 * 使用外部文件系统来控制内部程序关闭。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c05_gracefully_shutdown
 * @Class GracefullyShutdownDemo
 * @Date 2021/6/9 0009 15:17
 */
object GracefullyShutdownDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val streamingContext: StreamingContext = StreamingContext.getOrCreate("spark-streaming-checkpoint2", createSSC)

    // 监控并优雅关机 -> 计算节点不再接受新的数据，而是把现有数据处理完毕再关机
    // 需要创建新的线程进行优雅关闭
    new Thread(new MonitorStop(streamingContext)).start()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
   * 创建携带处理逻辑的StreamingContext
   *
   * @return
   */
  def createSSC(): StreamingContext = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GracefullyShutdownDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    // checkpoint
    streamingContext.checkpoint("spark-streaming-checkpoint2")

    streamingContext.socketTextStream("linux01", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount: Int = values.sum
        val previousCount: Int = state.getOrElse(0)
        Some(currentCount + previousCount)
      })
      .print()

    streamingContext
  }

  /**
   * 通过监控HDFS，如果存在stopSpark文件夹，则停止Spark Streaming任务
   *
   * @param streamingContext
   */
  class MonitorStop(streamingContext: StreamingContext) extends Runnable {
    override def run(): Unit = {
      val fs: FileSystem = FileSystem.get(new URI("hdfs://linux01:8020"), new Configuration(), "lanyangji")

      while (true) {
        try {
          TimeUnit.SECONDS.sleep(5)
        } catch {
          case ex: Exception => ex.printStackTrace()
        }

        val exists: Boolean = fs.exists(new Path("hdfs://linux01:8020/stopSpark"))
        if (exists) {
          val state: StreamingContextState = streamingContext.getState()
          if (state == StreamingContextState.ACTIVE) {
            streamingContext.stop(stopSparkContext = true, stopGracefully = true)
            System.exit(0)
          }
        }
      }
    }
  }

}
