package cn.spark.streaming.c04_sink

import cn.spark.streaming.c02_source.D02_KafkaDemo.KafkaPerson
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement}

/**
 * 输出操作指定了对流数据经转化操作得到的数据所要执行的操作(例如把结果推入外部数据库
 * 或输出到屏幕上)。与 RDD 中的惰性求值类似，如果一个 DStream 及其派生出的 DStream 都没
 * 有被执行输出操作，那么这些 DStream 就都不会被求值。如果 StreamingContext 中没有设定输出
 * 操作，整个 context 就都不会启动。
 * 输出操作如下：
 *
 * ➢  print()：在运行流程序的驱动结点上打印 DStream 中每一批次数据的最开始 10 个元素。这
 * 用于开发和调试。在 Python API 中，同样的操作叫 print()。
 *
 * ➢  saveAsTextFiles(prefix, [suffix])：以 text 文件形式存储这个 DStream 的内容。每一批次的存
 * 储文件名基于参数中的 prefix 和 suffix。”prefix-Time_IN_MS[.suffix]”。
 *
 * ➢  saveAsObjectFiles(prefix, [suffix])：以 Java 对象序列化的方式将 Stream 中的数据保存为
 * SequenceFiles . 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". Python
 * 中目前不可用。
 *
 * ➢  saveAsHadoopFiles(prefix, [suffix])：将 Stream 中的数据保存为 Hadoop files. 每一批次的存
 * 储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]"。Python API 中目前不可用。
 *
 * ➢  foreachRDD(func)：这是最通用的输出操作，即将函数 func 用于产生于 stream 的每一个
 * RDD。其中参数传入的函数 func 应该实现将每一个 RDD 中数据推送到外部系统，如将
 * RDD 存入文件或者通过网络将其写入数据库。
 * 通用的输出操作 foreachRDD()，它用来对 DStream 中的 RDD 运行任意计算。这和 transform()
 * 有些类似，都可以让我们访问任意 RDD。在 foreachRDD()中，可以重用我们在 Spark 中实现的
 * 所有行动操作。比如，常见的用例之一是把数据写到诸如 MySQL 的外部数据库中。
 * 注意：
 * 1)  连接不能写在 driver 层面（序列化）
 * 2)  如果写在 foreach 则每个 RDD 中的每一条数据都创建，得不偿失；
 * 3)  增加 foreachPartition，在分区创建（获取）。
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c04_sink
 * @Class D01_SinkMySqlDemo
 * @Date 2021/6/9 0009 11:28
 */
object D01_SinkMySqlDemo {
  def main(args: Array[String]): Unit = {
    // 将数据处理后写到mysql
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("D01_SinkMySqlDemo")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.socketTextStream("linux01", 9999)
      .mapPartitions((jsons: Iterator[String]) => {
        val objectMapper = new ObjectMapper()
        jsons.map((json: String) => objectMapper.readValue(json, classOf[KafkaPerson]))
      })
      .foreachRDD((rdd: RDD[KafkaPerson]) => {
        rdd.foreachPartition((persons: Iterator[KafkaPerson]) => {
          // 这样相当于一个分区建议一个mysql连接，并且我们这里采用批量插入的方式
          val connection: Connection = ConnectionPool.getConnection
          connection.setAutoCommit(false)
          val preparedStatement: PreparedStatement = connection.prepareStatement("insert into tbl_user(name, age) values(?, ?)")
          persons.foreach((p: KafkaPerson) => {
            preparedStatement.setString(1, p.name)
            preparedStatement.setInt(2, p.age)
            preparedStatement.addBatch()
          })
          preparedStatement.executeBatch()
          connection.commit()
          ConnectionPool.returnConnection(connection)
        })
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
