package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class MapPartitionWithIndexDemo
 * @Date 2021/5/31 0031 18:42
 */
object MapPartitionWithIndexDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("MapPartitionWithIndexDemo"))
    sc.textFile("input/")
      .flatMap(_.split(" "))
      .mapPartitionsWithIndex((index, datas) => datas.map((index, _)))
      .collect()
      .foreach(println(_))

    // 练习：只保留1号分区的数据
    sc.textFile("input/1*.txt")
      // flatMap扁平化处理，将每个元素拆成多个
      .flatMap(_.split(" "))
      .mapPartitionsWithIndex {
        case (index, iter) => {
          if (index == 1) {
            iter
          } else {
            // 否则，返回空迭代器
            Nil.iterator
          }
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }

}
