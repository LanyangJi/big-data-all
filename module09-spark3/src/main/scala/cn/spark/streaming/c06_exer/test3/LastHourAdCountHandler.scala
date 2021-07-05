package cn.spark.streaming.c06_exer.test3

import cn.spark.streaming.c06_exer.beans.AdsLog
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer.test3
 * @Class LastHourAdCountHandler
 * @Date 2021/6/11 0011 13:54
 */
object LastHourAdCountHandler {
  val format = new SimpleDateFormat("HH:mm")

  /**
   * 统计最近一小时(2 分钟)广告分时点击总数
   *
   * @param filterAdDStream
   * @return
   */
  def getAdHourMintToCount(filterAdDStream: DStream[AdsLog]): DStream[(String, List[(String, Long)])] = {
    // 1. 开窗
    val windowDStream: DStream[AdsLog] = filterAdDStream.window(Minutes(2))

    // 2. 转换数据结构
    val adHmAndOneDStream: DStream[((String, String), Long)] = windowDStream.map {
      adsLog => {
        val hm: String = format.format(new Date(adsLog.timestamp))
        ((adsLog.adId, hm), 1L)
      }
    }

    // 3. 统计
    val adHmAndCountDStream: DStream[((String, String), Long)] = adHmAndOneDStream.reduceByKey(_ + _)

    // 4. 转换数据结构
    val mapStream: DStream[(String, (String, Long))] = adHmAndCountDStream.map {
      case ((adId, hm), count) => {
        (adId, (hm, count))
      }
    }

    // 5. 分组
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = mapStream.groupByKey

    // 6. 排序
    val resultStream: DStream[(String, List[(String, Long)])] = groupDStream.mapValues {
      iter => {
        iter.toList.sortWith(_._1 < _._1)
      }
    }

    resultStream
  }

}
