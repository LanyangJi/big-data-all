package cn.spark.streaming.c06_exer.beans

/**
 * 城市信息表
 *
 * @param cityId   城市id
 * @param cityName 城市名
 * @param area     城市所在大区
 */
case class CityInfo(cityId: Long, cityName: String, area: String)
