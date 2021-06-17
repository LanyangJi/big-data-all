package cn.spark.sql.c05_hive

import cn.spark.sql.c01_source.beans.{People, Person}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * spark hive整合hive
 *
 * @Author jilanyang
 * @Package cn.spark.sql.c05_hive
 * @Class HiveDemo
 * @Date 2021/6/7 0007 22:15
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")
    System.setProperty("HADOOP_USER_NAME", "lanyangji")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HiveDemo")
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      // 在开发工具中创建数据库默认是在本地仓库，通过参数修改数据库仓库的地址
      .config("spark.sql.warehouse.dir", "hdfs://linux01:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql

    // 建表
    sql("drop table if exists tbl_person")
    sql("create table if not exists tbl_person(id int, name string, age int) row format delimited fields terminated by ','")
    // 导入数据
    sql("load data local inpath 'input/user.txt' into table tbl_person")

    // 查询 using hive sql
    sql("select * from tbl_person").show

    // 条件查询
    val sqlDf: DataFrame = sql("select * from tbl_person where id = 1")
    val strDs: Dataset[String] = sqlDf.map {
      case Row(id: Int, name: String, age: Int) => s"id: $id, name: $name, age: $age"
    }
    strDs.show()

    // 关联查询
    val personDf: DataFrame = sparkSession.createDataFrame((1 to 50).map(i => People(i, "person" + i, 23 + i)))
    personDf.createOrReplaceTempView("persons")
    sql("select * from persons p inner join tbl_person tp on p.id = tp.id").show()

    // 创建管理表
    sql("drop table if exists tbl_person_copy")
    sql("create table if not exists tbl_person_copy(id int, name string, age int) stored as parquet")
    val userDf: DataFrame = sparkSession.table("tbl_user")
    userDf.write.mode(SaveMode.Overwrite).saveAsTable("tbl_person_copy")
    sql("select * from tbl_person_copy;").show

    // 创建外部表关联本地数据
    // 本地D盘根目录下
    val dataDir = "/tmp/parquet_data"
    sparkSession.range(10).write.parquet(dataDir)
    sql("drop table if exists hive_bigints_demo")
    sql(s"create external table if not exists hive_bigints_demo(id bigint) stored as parquet location '$dataDir'")
    sql("select * from hive_bigints_demo").show

    sparkSession.stop()
  }
}
