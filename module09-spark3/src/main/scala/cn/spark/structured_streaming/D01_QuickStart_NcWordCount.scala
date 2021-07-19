package cn.spark.structured_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 结构化流是构建在Spark SQL引擎上的可扩展和容错流处理引擎。您可以像在静态数据上表示批处理计算一样来表示流计算。
 * Spark SQL引擎会不断地增量地运行它，并在流数据不断到达时更新最终结果。
 * 你可以使用Scala、Java、Python或R中的Dataset/DataFrame API来表示流聚合、事件时间窗口、流到批处理连接等。
 * 计算在同一个经过优化的Spark SQL引擎上执行。最后，系统通过检查点和写前日志确保端到端精确一次的容错保证。
 * 简而言之，结构化流提供了快速、可扩展、容错、端到端精确一次的流处理，而无需用户对流进行推理。
 *
 * 在内部，默认情况下，结构化流查询使用微批处理引擎处理，该引擎将数据流作为一系列小批处理作业处理，从而实现低至100毫秒的端到端延迟和正好一次的容错保证。
 * 然而，从Spark 2.3开始，我们引入了一种新的低延迟处理模式，称为连续处理(Continuous processing)，它可以实现端到端延迟低至1毫秒，
 * 并且至少有一次保证。无需更改查询中的Dataset/DataFrame操作，您将能够根据应用程序需求选择模式。
 *
 * @author lanyangji
 * @date 2021/7/16 10:41
 * @packageName cn.spark.structured_streaming 
 * @className D01_QuickStart_NcWordCount
 */
object D01_QuickStart_NcWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val spark: SparkSession = SparkSession.builder().master("local[3]").appName("D01_QuickStart_NcWordCount").getOrCreate()

    import spark.implicits._

    /*
      读取socket端口数据
      这行DataFrame表示一个包含流文本数据的无界表。
      该表包含一列名为value的字符串，流文本数据中的每一行都成为表中的一行。
      注意，由于我们只是设置转换，并且还没有开始转换，所以当前没有接收任何数据。
     */
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    // 修改日志级别
    // sparkSession.sparkContext.setLogLevel("WARN")

    /*
     转换为ds
     接下来，我们使用.as[String]将DataFrame转换为字符串数据集，
     这样我们就可以应用flatMap操作将每行分割为多个单词。
     */
    val ds: Dataset[String] = df.as[String]

    /*
     结果单词数据集包含所有单词
     最后，wordCountDf DataFrame，方法是根据数据集中的惟一值分组并对它们进行计数。
     注意，这是一个流式数据帧，它表示流中运行的单词计数。
     */
    val wordCountDf: DataFrame = ds.flatMap(_.split(" "))
      .groupBy("value")
      .count()

    /*
    我们现在已经设置了对流数据的查询。剩下的就是实际开始接收数据并计算计数。
    为此，我们将其设置为在每次更新计数时将完整的计数集(由outputMode(“complete”)指定)打印到控制台。
    然后使用start()启动流计算。
     */
    val streamingQuery: StreamingQuery = wordCountDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    streamingQuery.awaitTermination();
  }
}
