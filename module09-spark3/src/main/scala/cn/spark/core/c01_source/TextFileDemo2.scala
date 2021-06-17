package cn.spark.core.c01_source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Administrator
 * @date 2021/6/13 0013 23:56
 * @packageName cn.spark.core.c01_source 
 * @className TextFileDemo2
 */
object TextFileDemo2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TextFileDemo2")
    val sc = new SparkContext(sparkConf)

    // 从文件中读取，通配符 + 所属文件
    // wholeTextFiles方法能够识别数据来自哪个文件，它是以文件问单位
    // 读取结果表示为元组：（文件全路径, 文件内容）
    val inputRdd: RDD[(String, String)] = sc.wholeTextFiles("input/1*.txt")
    //    inputRdd.collect().foreach(println)
    inputRdd.map {
      case (filePath, content) => {
        filePath + "->\n" + content
      }
    }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
