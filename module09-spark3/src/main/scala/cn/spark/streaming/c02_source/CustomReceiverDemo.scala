package cn.spark.streaming.c02_source

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

/**
 * 需要继承 Receiver，并实现 onStart、onStop 方法来自定义数据源采集。
 * 自定义数据源，实现监控某个端口号，获取该端口号内容。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c02_source
 * @Class CustomReceiverDemo
 * @Date 2021/6/8 0008 14:19
 */
object CustomReceiverDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomReceiverDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    streamingContext.receiverStream(new CustomReceiver("linux01", 9999))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  // 自定义数据接收器
  class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    //最初启动的时候，调用该方法，作用为：读数据并将数据发送给 Spark
    override def onStart(): Unit = {
      new Thread("socket receiver") {
        override def run(): Unit = {
          receiver()
        }
      }.start()
    }

    /**
     * 接受并发送给spark逻辑
     */
    def receiver(): Unit = {
      val socket = new Socket(host, port)
      // 接受端口传来的数据
      var input: String = ""
      // bufferedReader用于读取端口传来的数据
      val bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      // 读取一行
      input = bufferedReader.readLine()
      //当 receiver 没有关闭并且输入数据不为空，则循环发送数据给 Spark
      while (!isStopped() && input != null) {
        // 发送给spark
        store(input)
        // 继续读取下一行
        input = bufferedReader.readLine()
      }

      // 循环跳出则关闭资源
      bufferedReader.close()
      socket.close()

      // 重启
      restart("custom socket receiver restart")
    }

    override def onStop(): Unit = {
    }
  }
}
