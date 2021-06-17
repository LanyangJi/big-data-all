package cn.spark.streaming.c06_exer.beans

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer.beans
 * @Class AdsLog
 * @Date 2021/6/10 0010 15:01
 */
case class AdsLog(timestamp: Long, area: String, city: String, userId: String, adId: String)
