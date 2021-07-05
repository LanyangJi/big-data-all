package cn.spark.sql.c01_source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * dataFrame支持DSL风格，即domain-specific language 特定领域语言
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c01_source
 * @Class D05_DslDemo
 * @Date 2021/6/3 0003 17:02
 */
object D05_DslDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("D05_DslDemo")
      .getOrCreate()

    // 引入重要的包，sparkSession是SparkSession的对象名
    import sparkSession.implicits._

    val inputDataFrame: DataFrame = sparkSession.read.json("input/user.json")

    inputDataFrame.printSchema()

    // name
    inputDataFrame.select("name").show()

    // age + 1
    inputDataFrame.select($"name", $"age" + 1 as "new_age").show

    // 第二种写法
    inputDataFrame.select('name, 'age + 1 as "new_age").show()

    // age > 30
    inputDataFrame.filter($"age" > 40).show

    // group by age and count
    inputDataFrame.groupBy("age").count().show()

    sparkSession.stop()
  }
}
