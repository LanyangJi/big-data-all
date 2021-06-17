package cn.spark.streaming.c06_exer.test1

import cn.spark.streaming.c06_exer.beans.AdsLog
import cn.spark.streaming.c06_exer.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer.test1
 * @Class RealTimeApp
 * @Date 2021/6/10 0010 16:46
 */
object RealTimeApp extends App {
  System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

  private val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
  private val streamingContext = new StreamingContext(sparkConf, Seconds(5))

  // 读取数据
  private val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamingContext)

  private val adsLogDStream: DStream[AdsLog] = inputDStream.map {
    record => {
      val line: String = record.value()
      val fields: Array[String] = line.split("\t")
      // timestamp area  city  userId  adId
      AdsLog(fields(0).trim.toLong, fields(1), fields(2), fields(3), fields(4))
    }
  }

  // 过滤
  private val filterDStream: DStream[AdsLog] = BlackListHandler.filterByBlackList(adsLogDStream)
  // 写入
  BlackListHandler.addBlackList(filterDStream)

  // 测试打印
  filterDStream.cache()
  filterDStream.count().print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
