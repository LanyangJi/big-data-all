package cn.spark.core.c04_operator_kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class PartitionByDemo
 * @Date 2021/6/2 0002 9:50
 */
object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("PartitionByDemo"))
    /*
      如果重分区的分区器和当前 RDD 的分区器一样，则不会执行任何操作
      if (self.partitioner == Some(partitioner)) {
        self
      } else {
        new ShuffledRDD[K, V, V](self, partitioner)
      }

      hash分区
      def nonNegativeMod(x: Int, mod: Int): Int = {
        val rawMod = x % mod
        rawMod + (if (rawMod < 0) mod else 0)
      }
     */
    sc.makeRDD(List("tom" -> 1, "amy" -> 2, "blank" -> 3), 3)
      .partitionBy(new HashPartitioner(2))
      .mapPartitionsWithIndex {
        case (index, datas) => datas.map((index, _))
      }
      .foreach(println)

    sc.stop()
  }

}
