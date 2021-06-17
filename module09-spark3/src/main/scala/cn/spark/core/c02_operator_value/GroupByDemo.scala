package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样
 * 的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
 * 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class GroupByDemo
 * @Date 2021/5/31 0031 19:01
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("GroupByDemo"))
    // 将首字母相同的放到同一个分组中
    sc.textFile("input/1*.txt")
      .flatMap(_.split(" "))
      // 将结果作为分组的key
      .groupBy(_.substring(0, 1))
      .collect()
      .foreach(println(_))

    println()

    // 练习：从服务器日志数据 apache.log 中获取每个时间段访问量
    sc.textFile("input/apache.log")
      // 精确分钟
      .map(line => {
        val fields: Array[String] = line.split(" ")
        val time: String = fields(3)
        (time.substring(0, 16), 1)
      })
      .groupBy(_._1)
      .map {
        case (time, iter) => {
          (time, iter.size)
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }

}
