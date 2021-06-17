package cn.spark.streaming.c06_exer.utils

import java.io.InputStream
import java.util.Properties

/**
 * 加载配置文件工具类
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer
 * @Class PropertyUtil
 * @Date 2021/6/9 0009 22:36
 */
object PropertyUtil {
  def load(propertyName: String): Properties = {
    val properties = new Properties()
    var inputStream: InputStream = null
    try {
      inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyName)
      properties.load(inputStream)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      if (inputStream != null)
        inputStream.close()
    }
    properties
  }
}
