package cn.spark.core.c11_partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 自定义分区器
 *
 * @author Administrator
 * @date 2021/6/16 0016 17:10
 * @packageName cn.spark.core.c11_partitioner 
 * @className CustomPartitionerDemo
 */
object CustomPartitionerDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomPartitionerDemo")
    val sc = new SparkContext(sparkConf)

    val inputRdd: RDD[(String, Int)] = sc.makeRDD(
      List(("nba", 1), ("cba", 2), ("wnba", 3), ("cuba", 4), ("nbl", 5)),
      3
    )

    // 使用自定义分区器
    val partitionRdd: RDD[(String, Int)] = inputRdd.partitionBy(new MyPartitioner(4))

    partitionRdd.saveAsTextFile("output/partitionBy")
    sc.stop()
  }

  // 自定义分区器，分区器只能作用于kv类型数据
  class MyPartitioner(partitionNums: Int) extends Partitioner {
    // 分区数量
    override def numPartitions: Int = partitionNums

    // 根据数据的key值来返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case "wnba" => 2
        case _ => 3
      }
    }
  }
}
