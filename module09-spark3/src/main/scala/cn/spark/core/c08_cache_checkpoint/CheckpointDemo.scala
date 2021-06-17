package cn.spark.core.c08_cache_checkpoint

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 所谓的检查点其实就是通过将 RDD 中间结果写入磁盘
 * 由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点
 * 之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 * 对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。
 *
 *
 * 缓存和检查点区别
 * 1）Cache 缓存只是将数据保存起来，不切断血缘依赖。Checkpoint 检查点切断血缘依赖。
 * 2）Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint 的数据通常存
 * 储在 HDFS 等容错、高可用的文件系统，可靠性高。
 * 3）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存
 * 中读取数据即可，否则需要再从头计算一次 RDD。
 *
 * 注意：cache()方法虽然也可以存储也磁盘，但那些是临时文件，在作业执行完毕之后会被删除，因此不需要显式地指定目录。
 * checkpoint检查点会被永久保存，不会被删除，所以需要显式地指定检查点存储目录。
 *
 * @Author jilanyang
 * @Package cn.spark.core.c08_cache_checkpoint
 * @Class CheckpointDemo
 * @Date 2021/6/2 0002 16:56
 */
object CheckpointDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CacheDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 设置检查点目录, 也可存储于hdfs中
    sc.setCheckpointDir("./spark-checkpoint1")

    val wordAndOneRdd: RDD[(String, Int)] = sc.textFile("input/word.txt")
      .flatMap(_.split(" "))
      .map(word => {
        println("map...")
        (word, 1)
      })

    // 打印血缘关系
    println(wordAndOneRdd.toDebugString)

    // 增加缓存，避免再重新跑一个 job 做 checkpoint
    wordAndOneRdd.cache()
    //// 数据检查点：针对 wordToOneRdd 做检查点计算
    wordAndOneRdd.checkpoint()

    wordAndOneRdd.reduceByKey(_ + _)
      .foreach(println)

    // 打印血缘关系, checkpoint会切断血缘关系，这是与cache()不一样
    println(wordAndOneRdd.toDebugString)

    sc.stop()
  }

}
