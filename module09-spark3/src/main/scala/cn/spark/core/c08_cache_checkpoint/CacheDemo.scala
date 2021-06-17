package cn.spark.core.c08_cache_checkpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存
 * 在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算
 * 子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。
 *
 * 缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD 的缓存容错机
 * 制保证了即使缓存丢失也能保证计算的正确执行。通过基于 RDD 的一系列转换，丢失的数
 * 据会被重算，由于 RDD 的各个 Partition 是相对独立的，因此只需要计算丢失的部分即可，
 * 并不需要重算全部 Partition。
 * Spark 会自动对一些 Shuffle 操作的中间数据做持久化操作(比如：reduceByKey)。这样
 * 做的目的是为了当一个节点 Shuffle 失败了避免重新计算整个输入。但是，在实际使用的时
 * 候，如果想重用数据，仍然建议调用 persist 或 cache。
 *
 * @Author jilanyang
 * @Package cn.spark.core.c08_cache_checkpoint
 * @Class CacheDemo
 * @Date 2021/6/2 0002 16:52
 */
object CacheDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    val wordAndOneRdd: RDD[(String, Int)] = sc.textFile("input/word.txt")
      .flatMap(_.split(" "))
      .map(word => {
        println("map....")
        (word, 1)
      })

    // 缓存 def cache(): this.type = persist()
    wordAndOneRdd.cache()

    // 也是通过persist设置缓存策略
    //    wordAndOneRdd.persist(StorageLevel.MEMORY_AND_DISK)

    wordAndOneRdd.reduceByKey(_ + _)
      .foreach(println)

    println("----------分割线-----------")

    // 注意，RDD中并不存储数据，这边如果不加cache缓存，那么这边即使你用同一个wordAndOneRdd对象，
    // 也会从头开始计算以获取数据（对象重用，数据并没有重用--RDD不保存数据，需要用cache缓存）
    // 注释掉wordAndOneRdd.cache()可以查看map方法中打印几次
    wordAndOneRdd.groupByKey()
      .mapValues(iter => iter.size)
      .foreach(println)

    sc.stop()
  }

}
