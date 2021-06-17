package cn.spark.core.c04_operator_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 思考一个问题：reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别？
 * reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
 * FoldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相
 * 同
 *
 * AggregateByKey：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规
 * 则可以不相同
 * CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区
 * 内和分区间计算规则不相同。
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class CombineByKeyDemo
 * @Date 2021/6/2 0002 10:44
 */
object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val data = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CombineByKeyDemo")
    val sc = new SparkContext(sparkConf)

    // key, sum, count
    val combineRdd: RDD[(String, (Int, Int))] = sc.makeRDD(data)
      .combineByKey(
        (_, 1), // 分区内第一个元素的初始化操作
        (t: (Int, Int), v: Int) => (t._1 + v, t._2 + 1), // 分区内聚合操作
        (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2) // 分区间聚合操作
      )

    combineRdd.mapValues(t => t._1 / t._2)
      .foreach(println(_))

    /*
        reduceByKey:
            combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
        aggregateByKey:
            combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),cleanedSeqOp, combOp, partitioner)
        foldByKey:
            combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),cleanedFunc, cleanedFunc, partitioner)
        combineByKey:
            combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,partitioner, mapSideCombine, serializer)(null)
     */

    sc.stop()
  }

}
