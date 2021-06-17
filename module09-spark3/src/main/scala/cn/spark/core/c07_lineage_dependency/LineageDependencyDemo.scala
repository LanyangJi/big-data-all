package cn.spark.core.c07_lineage_dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * rdd的血缘关系
 * toDebugString
 *
 * 依赖关系
 * dependencies
 *
 * 窄依赖表示每一个父(上游)RDD 的 Partition 最多被子（下游）RDD 的一个 Partition 使用，
 * 窄依赖我们形象的比喻为独生子女
 * 宽依赖表示同一个父（上游）RDD 的 Partition 被多个子（下游）RDD 的 Partition 依赖，会
 * 引起 Shuffle，总结：宽依赖我们形象的比喻为多生。
 *
 * RDD 任务切分中间分为：Application、Job、Stage 和 Task
 * ⚫  Application：初始化一个 SparkContext 即生成一个 Application；
 * ⚫  Job：一个 Action 算子就会生成一个 Job；
 * ⚫  Stage：Stage 等于宽依赖(ShuffleDependency)的个数加 1(ResultStage)；
 * ⚫  Task：一个 Stage 阶段中，最后一个 RDD 的分区个数就是 Task 的个数。
 * 注意：Application->Job->Stage->Task 每一层都是 1 对 n 的关系。
 *
 * @Author jilanyang
 * @Package c07_lineage
 * @Class LineageDemo
 * @Date 2021/6/2 0002 15:48
 */
object LineageDependencyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LineageDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRdd: RDD[String] = sc.textFile("input/word.txt")
    println("1. " + dataRdd.toDebugString)
    println("dependency----> " + dataRdd.dependencies)

    /*
    RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存
    在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算
    子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。
     */
    dataRdd.cache()

    // 也可以转换存储级别
    // dataRdd.persist(StorageLevel.MEMORY_AND_DISK)

    val flatMapRdd: RDD[String] = dataRdd.flatMap(_.split(" "))
    println("2. " + flatMapRdd.toDebugString)
    println("dependency----> " + flatMapRdd.dependencies)

    val wordAndOneRdd: RDD[(String, Int)] = flatMapRdd.map((_, 1))
    println("3." + wordAndOneRdd.toDebugString)
    println("dependency----> " + wordAndOneRdd.dependencies)

    val wordAndCountRdd: RDD[(String, Int)] = wordAndOneRdd.reduceByKey(_ + _)
    println("4." + wordAndCountRdd.toDebugString)
    println("dependency----> " + wordAndCountRdd.dependencies)

    val res: Array[(String, Int)] = wordAndCountRdd.collect()
    println(res.mkString(","))

    sc.stop()
  }
}
