package cn.spark.streaming.c02_source

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.beans.BeanProperty

/**
 * ReceiverAPI：需要一个专门的 Executor 去接收数据，然后发送给其他的 Executor 做计算。存在
 * 的问题，接收数据的 Executor 和计算的 Executor 速度会有所不同，特别在接收数据的 Executor
 * 速度大于计算的 Executor 速度，会导致计算数据的节点内存溢出。早期版本中提供此方式，当
 * 前版本不适用
 * DirectAPI：是由计算的 Executor 来主动消费 Kafka 的数据，速度由自身控制。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c02_source
 * @Class KafkaDemo
 * @Date 2021/6/8 0008 15:11
 */
object KafkaDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // kafka参数设置
    val kafkaConfigMap: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux01:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "lanyangji",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )

    // 读取kafka数据并创建DStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](Set("spark-kafka"), kafkaConfigMap)
    )

    // 将每条消息的kv取出并做word count
    //    kafkaStream.map(record => record.value())
    //      .flatMap(_.split(" "))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .print()

    // 接受并处理kafka中的json数据， 形如 {"name":"jilanyang", "age": 33}
    kafkaStream.map(record => record.value())
      .mapPartitions(jsons => {
        val mapper = new ObjectMapper()
        jsons.map(json => mapper.readValue(json, classOf[KafkaPerson]))
      })
      .map(p => s"name: ${p.name}, age: ${p.age}")
      .print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  class KafkaPerson {
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: Int = _
  }
}
