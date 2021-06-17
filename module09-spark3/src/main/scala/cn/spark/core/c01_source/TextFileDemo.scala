package cn.spark.core.c01_source

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package cn.spark.core.c01_source
 * @Class TextFileDemo
 * @Date 2021/5/31 0031 15:51
 */
object TextFileDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("TextFileDemo"))

    // 1. textFile参数可以是文件，也可以是目录，也是可以通配符（input/1*.txt）
    //    可以使本地绝对路径，也可是相对路径；
    //    还可以是分布式文件系统的路径，比如hdfs://linux01:8020/input，hBase等
    // 2. textFile也支持分区，默认minPartitions，
    //    等于defaultMinPartitions = math.min(defaultParallelism, 2)
    //  注意：这里仅仅指的是最小分区，真正的分区数要大于等于设置的最小分区数
    //  Spark读取文件，默认的就是使用hadoop的方式读取文件
    //  实际分区数目的计算方法：totalSize
    sc.textFile("input")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println(_))

    sc.stop()
  }
}
