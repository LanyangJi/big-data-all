package cn.spark.streaming.c06_exer

import cn.spark.streaming.c06_exer.beans.CityInfo
import cn.spark.streaming.c06_exer.utils.PropertyUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import java.util.{Properties, Random}
import scala.collection.mutable.ArrayBuffer

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer
 * @Class MockerRealTime
 * @Date 2021/6/10 0010 10:37
 */
object MockerRealTime {
  /**
   * 模拟生成实时数据
   *
   * 格式 timestamp area  city  userId  adId
   * 某个时间点 所在大区  城市  某用户 某广告
   *
   * @return
   */
  def generateMockData(): Array[String] = {
    val arrayBuffer: ArrayBuffer[String] = ArrayBuffer[String]()

    val cityRandomOptions: RandomOptions[CityInfo] = RandomOptions(RandOpt(CityInfo(1, "北京", "华北"), 30),
      RandOpt(CityInfo(2, "上海", "华东"), 30),
      RandOpt(CityInfo(3, "广州", "华南"), 10),
      RandOpt(CityInfo(4, "深圳", "华南"), 20),
      RandOpt(CityInfo(5, "天津", "华北"), 10))

    val random = new Random
    val mapper = new ObjectMapper()
    // scala样例类进行json序列化需要先引入隐式转换
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    // 模拟实时数据 timestamp area  city  userId  adId
    for (elem <- 1 to 50) {
      val timestamp = System.currentTimeMillis()
      val cityInfo = cityRandomOptions.getRandomOpt
      val area = cityInfo.area
      val userId = 1 + random.nextInt(6)
      val adId = 1 + random.nextInt(6)

      arrayBuffer += String.join("\t", String.valueOf(timestamp),
        area, Serialization.write(cityInfo), String.valueOf(userId), String.valueOf(adId))
    }

    arrayBuffer.toArray
  }


  /**
   * 创建kafkaProducer
   *
   * @param brokers
   * @return
   */
  def createKafkaProducer(brokers: String): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[String, String](properties)
  }

  def main(args: Array[String]): Unit = {
    //    val json: String = "{\"cityId\":3,\"cityName\":\"广州\",\"area\":\"华南\"}"
    //    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    //    val info: CityInfo = Serialization.read[CityInfo](json)
    //    println(info)

    // 加载配置
    val properties: Properties = PropertyUtil.load("config.properties")
    val brokers: String = properties.getProperty("kafka.broker.list")
    val topic: String = "ads_log"

    val producer: KafkaProducer[String, String] = createKafkaProducer(brokers)
    while (true) {
      // 随机产生实时数据，并通过kafkaProducer发送到kafka
      for (line <- generateMockData()) {
        producer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }

      try {
        Thread.sleep(2000)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
  }
}
