package cn.spark.core.c05_action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 所谓的行动算子，就是触发作业执行的方法，最终底层触发的是上下文对象的sc.runJob方法
 * 再底层是调用了dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
 * 再底层调用了submitJob(rdd, func, partitions, callSite, resultHandler, properties)提交作业
 * 最终递交的作业就是sparkContext对象的方法handleJobSubmitted中的val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
 *
 * 在驱动程序中，以数组 Array 的形式返回数据集的所有元素
 *
 * @Author jilanyang
 * @Package cn.spark.core.c05_action
 * @Class CollectDemo
 * @Date 2021/6/2 0002 14:13
 */
object CollectDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CollectDemo"))
    val res: Array[Int] = sc.makeRDD(1 to 5)
      // 将不同分区的数据按照分区顺序采集到driver端的内存中
      .collect()
    println(res.mkString(","))

    sc.stop()
  }

}
