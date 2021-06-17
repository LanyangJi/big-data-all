package cn.spark.streaming.c06_exer.test2

import cn.spark.streaming.c06_exer.beans.AdsLog
import cn.spark.streaming.c06_exer.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * 广告点击量实时统计
 * 描述：实时统计每天各地区各城市各广告的点击总流量，并将其存入 MySQL。
 * 思路分析
 * 1）单个批次内对数据进行按照天维度的聚合统计;
 * 2）结合 MySQL 数据跟当前批次数据更新原有的数据。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer.test2
 * @Class DateAreaCityAdCountHandler
 * @Date 2021/6/11 0011 10:56
 */
object DateAreaCityAdCountHandler {
  val format = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 统计每天各大区各个城市广告点击总数并保存至 MySQL 中
   *
   * @param filterAdDStream
   */
  def saveDateAreaCityAdCountToMysql(filterAdDStream: DStream[AdsLog]) = {
    //1.统计每天各大区各个城市广告点击总数
    val countDStram: DStream[((String, String, String, String), Int)] = filterAdDStream.map {
      adsLog => {
        val dt = format.format(new Date(adsLog.timestamp))
        // 写出
        ((dt, adsLog.area, adsLog.city, adsLog.adId), 1)
      }
    }.reduceByKey(_ + _)

    // 按批次写入mysql
    countDStram.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          iter => {
            val connection: Connection = JdbcUtil.getConnection()
            // 写库
            iter.foreach {
              case ((dt, area, city, adId), count) => {
                JdbcUtil.executeUpdate(
                  connection,
                  """
                    |insert into area_city_ad_count(dt, area, city, ad_id, count)
                    |values(?, ?, ?, ?)
                    |on duplicate key
                    |update count = count + ?""".stripMargin,
                  Array(dt, area, city, adId, count, count)
                )
              }
            }
            connection.close()
          }
        }
      }
    }
  }
}
