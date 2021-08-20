package cn.jly.bigdata.flink_advanced.sql;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 先看table api，再学习sql
 * 因为在table api的前几个示例中，展示了一些比较全的东西
 * {@link cn.jly.bigdata.flink_advanced.table.D01_TableApi_DataStream}
 * {@link cn.jly.bigdata.flink_advanced.table.D02_TableApi_Sql}
 * {@link cn.jly.bigdata.flink_advanced.table.D03_SQL_Window}
 * {@link cn.jly.bigdata.flink_advanced.table.D04_TableApi_Window}
 * {@link cn.jly.bigdata.flink_advanced.table.D05_TableApi_Sql_Kafka}
 * 等等
 * <p>
 * 与所有 SQL 引擎一样，Flink 查询在表之上运行。它不同于传统的数据库，因为 Flink 不在本地管理静态数据；相反，它的查询在外部表上连续运行。
 * <p>
 * Flink 数据处理管道从源表开始。源表产生在查询执行期间操作的行；它们是在查询的 FROM 子句中引用的表。
 * 这些可能是 Kafka 主题、数据库、文件系统或 Flink 知道如何使用的任何其他系统。
 * <p>
 * 可以通过 SQL 客户端或使用环境配置文件定义表。 SQL 客户端支持类似于传统 SQL 的 SQL DDL 命令。标准 SQL DDL 用于创建、更改、删​​除表。
 * <p>
 * ================= 动态表和连续查询的概念 =============================================
 * <p>
 * 虽然最初设计时并未考虑流语义，但 SQL 是构建连续数据管道的强大工具。 Flink SQL 与传统数据库查询的不同之处在于，它在行到达时不断消耗行并对其结果进行更新。
 * 连续查询永远不会终止并因此产生动态表。动态表是 Flink 的 Table API 和 SQL 支持流式数据的核心概念。
 * 连续流上的聚合需要在查询执行期间连续存储聚合结果。例如，假设您需要从传入的数据流中计算每个部门的员工人数。查询需要维护每个部门的最新计数，以便在处理新行时及时输出结果。
 * <p>
 * 这样的查询被认为是有状态的。 Flink 先进的容错机制会保持内部状态和一致性，因此即使面对硬件故障，查询也始终返回正确的结果。
 *
 * @author jilanyang
 * @createTime 2021/8/20 17:04
 */
public class D01_Sql_QuickStart {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 展示所有支持的函数
        TableResult result = tableEnv.executeSql("show functions");
        result.print();

        TableResult showTables = tableEnv.executeSql("show tables");
        showTables.print();

        Table table = tableEnv.sqlQuery("select 'hello world' as greet");
        tableEnv.toAppendStream(table, Row.class).printToErr("query");

        // 这些函数在开发 SQL 查询时为用户提供了强大的功能工具箱。例如，CURRENT_TIMESTAMP 将打印机器在执行时的当前系统时间——UTC时间，比本地时区晚8小时
        Table table2 = tableEnv.sqlQuery("select current_timestamp as process_time");
        tableEnv.toAppendStream(table2, Row.class).printToErr("process_time");

        env.execute("D01_Sql_QuickStart");
    }
}
