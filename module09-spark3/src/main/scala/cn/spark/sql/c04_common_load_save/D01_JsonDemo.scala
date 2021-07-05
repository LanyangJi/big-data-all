package cn.spark.sql.c04_common_load_save

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Spark SQL 能够自动推测 JSON 数据集的结构，并将它加载为一个 Dataset[Row]. 可以
 * 通过 SparkSession.read.json()去加载 JSON 文件。
 * 注意：Spark 读取的 JSON 文件不是传统的 JSON 文件，每一行都应该是一个 JSON 串。格
 * 式如下：
 * {"name":"Michael"}
 * {"name":"Andy"， "age":30}
 * [{"name":"Justin"， "age":19},{"name":"Justin"， "age":19}]
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c04_common_load_save
 * @Class JsonDemo
 * @Date 2021/6/7 0007 9:42
 */
object D01_JsonDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JsonDemo")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val inputDf: DataFrame = sparkSession.read.json("input/user.json")
    inputDf.createOrReplaceTempView("user")
    val queryDf: DataFrame = sparkSession.sql("select * from user where age between 20 and 40")
    queryDf.show()

    // 保存为json文件
    queryDf.write.mode(SaveMode.Overwrite).json("output/user.json")

    sparkSession.stop()
  }
}
