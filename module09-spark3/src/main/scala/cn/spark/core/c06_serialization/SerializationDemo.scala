package cn.spark.core.c06_serialization

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包检测
 * 从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor
 * 端执行。那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就
 * 形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给 Executor
 * 端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列
 * 化，这个操作我们称之为闭包检测。Scala2.12 版本后闭包编译方式发生了改变
 *
 * @Author jilanyang
 * @Package cn.spark.core.c06_serialization
 * @Class SerailizationDemo
 * @Date 2021/6/2 0002 15:00
 */
object SerializationDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SerializationDemo")
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRdd: RDD[String] = sc.makeRDD(List("tom", "amy", "kobe"))
    val search = new Search("o")
    val resRdd: RDD[String] = search.getMatch(dataRdd)
    resRdd.collect().foreach(println)

    sc.stop()
  }

  // 样例类在编译的时候会自动实现序列化接口，不需要手动去混入序列化接口
  class Search(queryStr: String) extends Serializable {
    def isContains(str: String): Boolean = {
      str.contains(queryStr)
    }

    def getMatch(dataRdd: RDD[String]): RDD[String] = {
      // 这边算子中用到了 this.queryStr和this.isContains，所以需要this可以序列化
      dataRdd.filter(isContains)
      // 改造方式二
      //      val q = queryStr
      //      dataRdd.filter(_.contains(q))
    }
  }

}
