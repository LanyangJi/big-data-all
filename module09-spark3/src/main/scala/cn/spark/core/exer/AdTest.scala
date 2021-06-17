package cn.spark.core.exer

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计出每一个省份 每个广告被点击数量排行的 Top3
 *
 * 时间戳 省份id  城市id  用户id 广告id(一条记录是一次点击事件)
 * 1516609143867 6 7 64 16
 * 1516609143869 9 4 75 18
 * 1516609143869 1 7 87 12
 *
 * @Author jilanyang
 * @Package cn.spark.core.exer
 * @Class AdTest
 * @Date 2021/6/2 0002 13:45
 */
object AdTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    sc.textFile("input/agent.log")
      .map((line: String) => {
        val fields: Array[String] = line.split(" ")
        // 提取出(（省份id,广告id）, 1)
        ((fields(1), fields(fields.length - 1)), 1)
      })
      .reduceByKey(_ + _) // ((省份id,广告id), count)
      .map((ele: ((String, String), Int)) => (ele._1._1, (ele._1._2, ele._2))) // 转换成(省份id, (广告id, count))
      .groupByKey() // 按照省份id聚合，并排序选出前三
      .mapValues(
        // 按照count排序并取出前三
        iter => iter.toList.sortBy(_._2).reverse.take(3)
      )
      .collect()
      .foreach(println)

    sc.stop()
  }
}
