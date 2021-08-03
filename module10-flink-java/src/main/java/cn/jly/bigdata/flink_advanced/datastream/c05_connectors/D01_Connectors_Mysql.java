package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import cn.jly.bigdata.flink_advanced.datastream.beans.TblUser;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * flink提供的官方连接器 —— mysql
 * 官方文档：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/jdbc/
 *
 * <p>
 * 这个连接器提供了一个将数据写入JDBC数据库的接收器。
 * 要使用它，请将以下依赖项添加到项目中（以及JDBC驱动程序）：
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-jdbc_2.11</artifactId>
 * <version>1.13.0</version>
 * </dependency>
 * <p>
 * JDBC接收器至少提供一次保证。不过，通过构造upsertsql语句或幂等SQL更新，可以有效地实现一次。
 * 配置如下（另请参见JdbcSink javadoc）。
 * 接收器从用户提供程序SQL字符串构建一个JDBC准备的语句，例如：
 * INSERT INTO some_table field1, field2 values (?, ?)
 * 然后它反复调用用户提供的函数，用流的每个值更新准备好的语句，例如：
 * (preparedStatement, someRecord) -> { ... update here the preparedStatement with values from someRecord ... }
 * <p>
 * SQL DML语句是成批执行的，可以选择使用以下实例进行配置（另请参见jdbcecutionoptions javadoc）
 * JdbcExecutionOptions.builder()
 * .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
 * .withBathSize(1000)                   // optional: default = 5000 values
 * .withMaxRetries(5)                    // optional: default = 3
 * .build()
 * <p>
 * 只要满足以下条件之一，就会执行JDBC批处理：
 * 1 已过配置的批处理间隔时间
 * 2 已达到最大批量大小
 * 3 Flink检查站已经启动
 *
 * 从 1.13 开始，Flink JDBC sink 支持 Exactly-once 模式。该实现依赖于 XA 标准的 JDBC 驱动程序支持。
 * 注意：在 1.13 中，Flink JDBC sink 不支持与 MySQL 或其他不支持每个连接多个 XA 事务的数据库的恰好一次模式。我们将改进 FLINK-22239 中的支持。
 * 要使用它，请使用上述 exactOnceSink() 方法创建一个接收器，并额外提供：
 * 1 仅一次选项
 * 2 执行选项
 * 3 XA 数据源供应商
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @class D01_Connectors_Mysql
 * @date 2021/7/26 22:24
 */
public class D01_Connectors_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStreamSource<TblUser> userDs = env.fromElements(
                new TblUser("王二麻", 25),
                new TblUser("陈紫函", 35)
        );

        // 声明jdbcSink
        SinkFunction<TblUser> jdbcSink = JdbcSink.sink(
                "insert into tbl_user(name, age) values(?,?)",
                (JdbcStatementBuilder<TblUser>) (preparedStatement, tblUser) -> {
                    preparedStatement.setString(1, tblUser.getName());
                    preparedStatement.setInt(2, tblUser.getAge());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000) // 最大批量大小
                        .withBatchIntervalMs(200) // 配置的批处理间隔时间
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://linux01:3306/test")
                        .withDriverName(Driver.class.getName())
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        // 执行sink
        userDs.addSink(jdbcSink);

        env.execute("D01_Connectors_Mysql");
    }
}
