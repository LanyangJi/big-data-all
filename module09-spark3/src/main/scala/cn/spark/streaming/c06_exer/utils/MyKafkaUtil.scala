package cn.spark.streaming.c06_exer.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.lang
import java.util.Properties

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer
 * @Class MyKafkaUtil
 * @Date 2021/6/10 0010 13:48
 */
object MyKafkaUtil {

  // 加载配置
  private val properties: Properties = PropertyUtil.load("config.properties")

  // kafka集群地址
  private val broker: String = properties.getProperty("kafka.broker.list")

  // kafka消费者配置
  private val kafkaConfigMap: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "lanyangji",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest 自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: lang.Boolean)
  )

  /**
   * 创建 DStream，返回接收到的输入数据
   *
   * @param topic
   * @param streamingContext
   * @return
   */
  def getKafkaStream(topic: String, streamingContext: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    /*
      创建 DStream，返回接收到的输入数据
      LocationStrategies：根据给定的主题和集群地址创建 consumer
      LocationStrategies.PreferConsistent：持续的在所有 Executor 之间分配分区
      ConsumerStrategies：选择如何在 Driver 和 Executor 上创建和配置 Kafka Consumer
      ConsumerStrategies.Subscribe：订阅一系列主题
     */
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](Seq(topic), kafkaConfigMap)
    )
  }
}
