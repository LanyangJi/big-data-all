package cn.spark.sql.c01_source

import cn.spark.sql.c01_source.beans.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class DataSetDemo
 * @Date 2021/6/4 0004 11:01
 */
object DataSetDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("DataSetDemo").getOrCreate()
    import sparkSession.implicits._

    // Seq -> DataSet
    // 借助隐式转换，使得普通Seq可以调用toDS方法
    // 在实际使用的时候，很少用到把序列转换成DataSet，更多的是通过RDD来得到DataSet
    val inputDs: Dataset[Person] = Seq(Person("张三", 23), Person("李四", 33), Person("王五", 77), Person("赵六", 55)).toDS()
    inputDs.show()

    // rdd -> DataSet
    val inputDs2: Dataset[Person] = sparkSession.sparkContext.makeRDD(List(("张三", 23), ("李四", 33), ("王五", 77), ("赵六", 55)))
      .map(ele => Person(ele._1, ele._2))
      .toDS()
    inputDs2.show

    // DataSet -> rdd
    val rdd: RDD[Person] = inputDs2.rdd
    rdd.collect().foreach(println)

    // dataFrame -> dataSet
    val df: DataFrame = sparkSession.sparkContext.makeRDD(List(("张三", 23), ("李四", 33), ("王五", 77), ("赵六", 55)))
      .toDF("name", "age")
    val personDs: Dataset[Person] = df.as[Person]
    personDs.show()

    // dataSet -> dataFrame
    val personDf: DataFrame = personDs.toDF()
    personDf.show()

    sparkSession.stop()
  }
}
