package cn.spark.core.c10_broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.util.control.Breaks.{break, breakable}

/**
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个
 * 或多个 Spark 操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
 * 广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark 会为每个任务
 * 分别发送。
 *
 * @Author jilanyang
 * @Package cn.spark.core.c10_broadcast
 * @Class BroadcastDemo
 * @Date 2021/6/2 0002 21:13
 */
object BroadcastDemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6), ("d", 7))

    // 广告变量
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    rdd.map {
      case (key, num) => {
        var num2 = 0
        breakable {
          for ((k, v) <- broadcast.value) {
            if (k == key) {
              num2 = v
              break()
            }
          }
        }

        (key, (num, num2))
      }
    }
      .foreach(println)

    sc.stop()

  }
}
