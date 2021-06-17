package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
 * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出
 * 现数据倾斜。
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class FilterDemo
 * @Date 2021/5/31 0031 19:06
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("FilterDemo"))
    sc.textFile("input/")
      .flatMap(_.split(" "))
      .filter(_.startsWith("s"))
      .collect()
      .foreach(println(_))

    println()

    // 练习：从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
    sc.textFile("input/apache.log")
      .filter {
        line => {
          val fields: Array[String] = line.split(" ")
          val date: String = fields(3)
          date.startsWith("17/05/2015")
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
