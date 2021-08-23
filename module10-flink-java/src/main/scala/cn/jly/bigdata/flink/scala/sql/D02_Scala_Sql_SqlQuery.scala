package cn.jly.bigdata.flink.scala.sql
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{$, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * @author jilanyang
 * @date 2021/8/23 19:27
 */
object D02_Scala_Sql_SqlQuery {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[(Long, String, Int)] = env.socketTextStream("linux01", 9999)
      .map(
        line => {
          val fields: Array[String] = line.split(",")
          (fields(0).trim.toLong, fields(1), fields(2).trim.toInt)
        }
      )

    // 使用内联的方式，flink内部会自动调用table.toString方法将Table对象转换为表名
    val table: Table = tableEnv.fromDataStream(ds, $("userId"), $("username"), $("amount"))
    val resTable01: Table = tableEnv.sqlQuery(
      "select sum(amount) as sum_amount from " + table + " group by username"
    )
    tableEnv.toChangelogStream(resTable01).print("resTable01")

    // 创建视图的方式
    tableEnv.createTemporaryView("tbl_order", ds, $("userId"), $("name"), $("amount"))
    val resTable02: Table = tableEnv.sqlQuery(
      "select name, avg(amount) as avg_amount from tbl_order group by name"
    )
    tableEnv.toChangelogStream(resTable02).printToErr("resTable02");

    env.execute("D02_Scala_Sql_SqlQuery")
  }

}
