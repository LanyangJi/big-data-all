package cn.jly.bigdata.flink_advanced.sql;

/**
 * SELECT 语句和 VALUES 语句是使用 TableEnvironment 的 sqlQuery() 方法指定的。该方法将 SELECT 语句（或 VALUES 语句）的结果作为表返回。
 * Table 可以在后续的 SQL 和 Table API 查询中使用，转换为 DataStream，或写入 TableSink。 SQL 和 Table API 查询可以无缝混合，并进行整体优化并转换为单个程序。
 *
 * 为了访问 SQL 查询中的表，它必须在 TableEnvironment 中注册。可以从 TableSource、Table、CREATE TABLE 语句、DataStream 注册表。
 * 或者，用户也可以在 TableEnvironment 中注册目录以指定数据源的位置。
 *
 * @author jilanyang
 * @date 2021/8/21 18:24
 */
public class D02_Sql_SqlQuery {
    public static void main(String[] args) {

    }
}
