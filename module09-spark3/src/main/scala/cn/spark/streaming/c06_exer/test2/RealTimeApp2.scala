package cn.spark.streaming.c06_exer.test2

import cn.spark.streaming.c06_exer.beans.{AdsLog, CityInfo}
import cn.spark.streaming.c06_exer.test1.BlackListHandler
import cn.spark.streaming.c06_exer.utils.{JdbcUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import java.sql.Connection
import java.util.Date

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer.test2
 * @Class RealTimeApp2
 * @Date 2021/6/11 0011 11:15
 */
object RealTimeApp2 {
  // scala样例类进行json序列化需要先引入隐式转换
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp2")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 读取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamingContext)

    val filterDStream: DStream[AdsLog] = kafkaDStream.map {
      record => {
        val line: String = record.value()
        val fields: Array[String] = line.split("\t")
        val cityInfo: CityInfo = Serialization.read[CityInfo](fields(2))
        val dt = DateAreaCityAdCountHandler.format.format(new Date(fields(0).trim.toLong))
        // 写出
        AdsLog(fields(0).trim.toLong, cityInfo.area, cityInfo.cityName, fields(3), fields(4))
      }
    }
      // 过滤出用户不在黑名单中的点击事件
      .mapPartitions {
        iter => {
          val connection: Connection = JdbcUtil.getConnection()
          iter.filter {
            adsLog => {
              val exists: Boolean = JdbcUtil.isExist(
                connection,
                "select * from black_list where user_id = ?",
                Array(adsLog.userId)
              )
              !exists
            }
          }
        }
      }

    // 缓存
    filterDStream.cache()

    //对没有被加入黑名单的用户统计当前批次单日各个用户对各个广告点击的总次数,
    // 并更新至 MySQL
    // 之后查询更新之后的数据,判断是否超过 100 次。
    // 如果超过则将给用户加入黑名单
    BlackListHandler.addBlackList(filterDStream)

    // 统计每天各大区各个城市广告点击总数并保存至 MySQL 中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
