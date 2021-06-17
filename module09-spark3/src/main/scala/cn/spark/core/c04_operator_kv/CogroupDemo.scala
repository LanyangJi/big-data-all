package cn.spark.core.c04_operator_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class CogroupDemo
 * @Date 2021/6/2 0002 13:39
 */
object CogroupDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CogroupDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    val firstRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
    val secondRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1)))

    // 分组（group） + 连接(connect)
    val resRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = firstRdd.cogroup(secondRdd)
    resRdd.collect().foreach(println)

    sc.stop()
  }

}
