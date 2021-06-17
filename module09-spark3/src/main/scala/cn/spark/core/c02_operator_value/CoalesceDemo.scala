package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 * 当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少
 * 分区的个数，减小任务调度成本
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class CoalesceDemo
 * @Date 2021/5/31 0031 19:24
 */
object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CoalesceDemo"))

    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 6)
      .mapPartitionsWithIndex((index, datas) => datas.map(("分区：" + index, _)))
      .collect()
      .foreach(println(_))

    println()

    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 6)
      // 压缩分区, 默认不会被分区的数据打乱重新组合（即默认不会shuffle），这种情况下的
      // 数据分区可能导致数据不均衡。
      // 如果想要数据分配均衡，可以进行shuffle处理，即第二个参数为shuffle=true
      .coalesce(2)
      .mapPartitionsWithIndex((index, datas) => datas.map(("分区：" + index, _)))
      .collect()
      .foreach(println(_))

    sc.stop()
  }

}
