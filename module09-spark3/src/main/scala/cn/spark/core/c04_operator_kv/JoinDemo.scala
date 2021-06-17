package cn.spark.core.c04_operator_kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
 * (K,(V,W))的 RDD
 *
 * 多余的key会被丢弃
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class JoinDemo
 * @Date 2021/6/2 0002 13:31
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinDemo")
    val sc = new SparkContext(sparkConf)

    val firstRdd = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    // 相同的key会依次匹配，尽量避免有多个相同的key，极端状态是否会出现类似于笛卡尔积的后果？谨慎使用
    val secondRdd = sc.makeRDD(List((1, "tom"), (2, "amy"), (1, "james")))

    val joinRdd = firstRdd.join(secondRdd)
    joinRdd.collect().foreach(println(_))

    sc.stop()
  }

}
