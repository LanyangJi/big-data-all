package cn.spark.structured_streaming

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author lanyangji
 * @date 2021/7/16 14:35
 * @packageName cn.spark.structured_streaming 
 * @className D02_QuickStart_KafkaWordCount
 */
object D02_QuickStart_KafkaWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("D02_QuickStart_KafkaWordCount")
      .getOrCreate()

    // 修改日志级别
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "linux01:9092")
      // 订阅多个topic可以以逗号分隔
      .option("subscribe", "structured_streaming_test")
      // 模式匹配订阅多个
      //.option("subscribePattern", "topic.*")
      .load()

    // 类型转换
    val kafkaDs: Dataset[(String, String)] =
      kafkaDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val wordCountDf: DataFrame = kafkaDs.map(_._2)
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    val streamingQuery: StreamingQuery = wordCountDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    streamingQuery.awaitTermination();
  }
}
