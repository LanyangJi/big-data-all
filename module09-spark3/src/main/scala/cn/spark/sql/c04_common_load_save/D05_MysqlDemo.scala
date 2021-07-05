package cn.spark.sql.c04_common_load_save

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * Spark SQL 可以通过 JDBC 从关系型数据库中读取数据的方式创建 DataFrame，通过对
 * DataFrame 一系列的计算后，还可以将数据再写回关系型数据库中。如果使用 spark-shell 操
 * 作，可在启动 shell 时指定相关的数据库驱动路径或者将相关的数据库驱动放到 spark 的类
 * 路径下。
 * bin/spark-shell --jars mysql-connector-java-5.1.27-bin.jar
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c04_common_load_save
 * @Class MysqlDemo
 * @Date 2021/6/7 0007 16:55
 */
object D05_MysqlDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MysqlDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //    val optionMap = new util.HashMap[String, String]()
    //    optionMap.put("url", "jdbc:mysql://linux01:3006/test")
    //    optionMap.put("driver", "com.mysql.jdbc.Driver")
    //    optionMap.put("user", "root")
    //    optionMap.put("password", "123456")
    //    optionMap.put("dbtable", "tbl_user")
    // 通用方式
    //    val inputDf: DataFrame = sparkSession.read.format("jdbc").options(optionMap).load()

    // 简便方式
    val jdbcUrl = "jdbc:mysql://linux01/test"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    val inputDf: DataFrame = sparkSession.read.jdbc(jdbcUrl, "tbl_user", properties)
    inputDf.show()

    // 操作
    inputDf.createOrReplaceTempView("user")
    val sqlDf: DataFrame = sparkSession.sql("select * from user where age between 20 and 40")

    // 结果写入到表tbl_user_copy
    sqlDf.write.mode(SaveMode.Append).jdbc(jdbcUrl, "tbl_user_copy", properties);

    sparkSession.stop()
  }
}
