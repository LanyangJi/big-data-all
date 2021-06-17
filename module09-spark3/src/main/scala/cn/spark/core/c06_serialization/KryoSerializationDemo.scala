package cn.spark.core.c06_serialization

import cn.spark.core.c06_serialization.SerializationDemo.Search
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Java 的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也
 * 比较大。Spark 出于性能的考虑，Spark2.0 开始支持另外一种 Kryo 序列化机制。Kryo 速度
 * 是 Serializable 的 10 倍。当 RDD 在 Shuffle 数据的时候，简单数据类型、数组和字符串类型
 * 已经在 Spark 内部使用 Kryo 来序列化。
 * 注意：即使使用 Kryo 序列化，也要继承 Serializable 接口。
 *
 * @Author jilanyang
 * @Package cn.spark.core.c06_serialization
 * @Class KryoSerializationDemo
 * @Date 2021/6/2 0002 15:21
 */
object KryoSerializationDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KryoSerializationDemo")
      // 替换掉默认的序列化机制 org.apache.spark.serializer.JavaSerializer -> 利用对象输出流ObjectOutputStream
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo进行序列化的自定义类 -> kryo序列化后比java序列化体积更小，速度更快
      // kryo绕过了java的序列化规则，使用自己的序列化规则
      // （比如transient关键字修饰的属性，java序列化机制无法序列化，但是kryo可以）
      .registerKryoClasses(Array(classOf[Search]))

    val sc: SparkContext = new SparkContext(sparkConf)
    val dataRdd: RDD[String] = sc.textFile("input/word.txt").flatMap(_.split(" "))

    val search = new Search("k")
    val redRdd: RDD[String] = search.getMatch(dataRdd)
    redRdd.foreach(println)

    sc.stop()
  }
}
