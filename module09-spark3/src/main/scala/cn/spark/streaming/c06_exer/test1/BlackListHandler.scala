package cn.spark.streaming.c06_exer.test1

import cn.spark.streaming.c06_exer.beans.AdsLog
import cn.spark.streaming.c06_exer.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer
 * @Class BlackListHandler
 * @Date 2021/6/10 0010 15:17
 */
object BlackListHandler {

  // 格式化时间对象
  private val format = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(filterAdsLogDStream: DStream[AdsLog]): Unit = {
    //统计当前批次中单日每个用户点击每个广告的总次数
    //1.将数据接转换结构 ads_log=>((date,user,adId),1)
    val dateUserAdAndOneStream: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(adsLog => {
      // 将时间戳转换为日期
      val date: String = format.format(new Date(adsLog.timestamp))
      // 返回值
      ((date, adsLog.userId, adsLog.adId), 1L)
    })

    // 2. 统计当日每个用户点击每个广告的总次数
    val dateUserAdAndCountStream: DStream[((String, String, String), Long)] = dateUserAdAndOneStream.reduceByKey(_ + _)

    // 3. 写入mysql
    dateUserAdAndCountStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          iter => {
            val connection: Connection = JdbcUtil.getConnection()
            iter.foreach {
              case ((date, userId, adId), count) => {
                // 插入数据库
                JdbcUtil.executeUpdate(
                  connection,
                  // 没有则插入，有则更新
                  """
                    |insert into user_ad_count(dt, user_id, ad_id, count)
                    |values(?, ?, ?, ?)
                    |on duplicate key
                    |update count = count + ?
                    """.stripMargin,
                  Array(date, userId, adId, count, count)
                )

                // 如果单日某个用户对某个广告的点击次数超过30，则加入黑名单
                val clickCount: Long = JdbcUtil.getDataFromMysql(
                  connection,
                  "select `count` from user_ad_count where dt = ? and user_id = ? and ad_id = ?",
                  Array(date, userId, adId)
                )
                if (clickCount > 30) {
                  JdbcUtil.executeUpdate(
                    connection,
                    "insert into black_list(user_id) values(?) on duplicate key update user_id = ?",
                    Array(userId, userId)
                  )
                }
              }
            }
            connection.close()
          }
        }
      }
    }
  }

  /**
   * 过滤出不在黑名单中的数据
   *
   * @param adsLogDStream
   * @return
   */
  def filterByBlackList(adsLogDStream: DStream[AdsLog]) = {
    adsLogDStream.transform {
      rdd => {
        rdd.filter(adsLog => {
          val connection: Connection = JdbcUtil.getConnection()
          val bool: Boolean = JdbcUtil.isExist(connection, "select * from black_list where user_id = ?", Array(adsLog.userId))
          connection.close()
          !bool
        })
      }
    }
  }
}
