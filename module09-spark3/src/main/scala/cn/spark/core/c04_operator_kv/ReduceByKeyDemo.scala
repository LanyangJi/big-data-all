package cn.spark.core.c04_operator_kv

import org.apache.spark.{SparkConf, SparkContext}

/** 可以将数据按照相同的 Key 对 Value 进行聚合
 *
 * reduceByKey和groupByKey的区别
 * 从 shuffle 的角度：reduceByKey 和 groupByKey 都存在 shuffle 的操作，但是 reduceByKey
 * 可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的
 * 数据量，而 groupByKey 只是进行分组，不存在数据量减少的问题，reduceByKey 性能比较
 * 高。
 * 从功能的角度：reduceByKey 其实包含分组和聚合的功能。GroupByKey 只能分组，不能聚
 * 合，所以在分组聚合的场合下，推荐使用 reduceByKey，如果仅仅是分组而不需要聚合。那
 * 么还是只能使用 groupByKey
 *
 * @Author jilanyang
 * @Package c03_tansform_2value
 * @Class ReduceByKeyDemo
 * @Date 2021/6/2 0002 9:55
 */
object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ReduceByKeyDemo"))
    sc.textFile("input/")
      .flatMap(_.split(" "))
      .map((_, 1))
      // 如果key的数据只有一个，则不会参与计算，直接返回
      .reduceByKey(_ + _)
      .foreach(println(_))

    sc.stop()
  }

}
