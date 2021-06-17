package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据。
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class MapPartitionDemo
 * @Date 2021/5/31 0031 18:37
 */
object MapPartitionDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("MapPartitionDemo"))

    /*
        mapPartitions可以以分区为单位进行数据转换操作，
          但是它会将整个分区的数据加载到内存中进行引用
          如果数据没有被处理完，就一直不会被释放（存在对象引用），
          那么在内存较小，数据量较大的情况下，可能会内存溢出

     */
    sc.makeRDD(List(1, 2, 3, 4, 5))
      .mapPartitions(datas => datas.filter(_ % 2 == 0))
      .collect()
      .foreach(println(_))

    // 求分区最大值
    sc.makeRDD(List(1, 2, 3, 4, 5))
      .mapPartitions {
        iter => {
          List(iter.max).iterator
        }
      }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
