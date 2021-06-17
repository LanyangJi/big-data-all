package cn.spark.core.c01_source

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package c01
 * @Class MakeRddDemo
 * @Date 2021/5/31 0031 15:35
 */
object MakeRddDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MakeRddDemo")
    // 修改默认的并行度
    sparkConf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(sparkConf)

    // 指定并行度
    /*
      // length为数据大小, numSlices为指定的并行度
      并行度默认值为defaultParallelism，即scheduler.conf.getInt("spark.default.parallelism", totalCores)，
      如果没有配置的话，则默认值为totalCores即为当前环境的最大核数

      - rdd计算分区内的数据是一个一个执行逻辑，只有前面一个数据全部的逻辑执行完毕，
      才会执行下一个逻辑（相同的分区内，不同的分区（多线程或者多核情况）是并行）
      - 不同分区之间的计算是无序的，互不干扰

      // Sequences need to be sliced at the same set of index positions for operations like RDD.zip() to behave as expected
      def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        (0 until numSlices).iterator.map { i =>
          val start = ((i * length) / numSlices).toInt
          val end = (((i + 1) * length) / numSlices).toInt
          (start, end)
        }
      }

     */
    sc.makeRDD(List("tom", "amy", "james"), 2)
      .collect()
      .foreach(println(_))

    println("------------")
    println("默认的并行度为：" + sc.defaultParallelism)
    println("----------")

    sc.parallelize(List("kobe", "rose"))
      .collect()
      .foreach(println(_))

    sc.stop()
  }

}
