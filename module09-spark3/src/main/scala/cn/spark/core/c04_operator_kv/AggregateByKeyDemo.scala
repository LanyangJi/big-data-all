package cn.spark.core.c04_operator_kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将数据根据不同的规则进行分区内计算和分区间计算
 *
 * // TODO : 取出每个分区内相同 key 的最大值然后分区间相加
 * // aggregateByKey 算子是函数柯里化，存在两个参数列表
 * // 1. 第一个参数列表中的参数表示初始值
 * // 2. 第二个参数列表中含有两个参数
 * // 2.1 第一个参数表示分区内的计算规则
 * // 2.2 第二个参数表示分区间的计算规则
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class AggregateByKeyDemo
 * @Date 2021/6/2 0002 10:10
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyDemo"))

    // 取出每个分区内相同 key 的最大值然后分区间相加
    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("b", 5), ("c", 6), ("c", 1)), 2)
      .aggregateByKey(0)(math.max(_, _), _ + _)
      .foreach(println)

    println

    // 求每个key的平均值
    sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("b", 5), ("c", 6), ("c", 1)), 2)
      .aggregateByKey((0, 0))(
        (t: (Int, Int), v: Int) => (t._1 + v, t._2 + 1),
        (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
      )
      .mapValues {
        case (sum, count) => sum / count
      }
      .collect()
      .foreach(println)

    sc.stop()
  }

}
