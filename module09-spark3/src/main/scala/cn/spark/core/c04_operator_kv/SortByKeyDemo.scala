package cn.spark.core.c04_operator_kv

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordered

/**
 * sortByKey 分区排序
 *
 * @Author jilanyang
 * @Package c04_transform_kv
 * @Class SortByKeyDemo
 * @Date 2021/6/2 0002 11:01
 */
object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")
    val data = List((User(1, "tom"), "basketball"), (User(2, "angela"), "football"), (User(2, "lily"), "football"), (User(3, "amy"), "pingpang"))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SortByKeyDemo")
    val sc = new SparkContext(sparkConf)
    val sortRdd = sc.makeRDD(data, 1)
      .sortByKey(ascending = false, numPartitions = 1)

    println(sortRdd.getNumPartitions)
    sortRdd.foreach(println)

    sc.stop()

  }

  case class User(id: Int, name: String) extends Ordered[User] {
    /**
     * 先按照id升序排序，如果相同，则按照name排序
     *
     * @param that
     * @return
     */
    override def compare(that: User): Int = {
      if (id.equals(that.id))
        name.compare(that.name)
      else
        id.compare(that.id)
    }
  }

}

