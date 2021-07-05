package cn.jly.bigdata.flink.datastream.c04_sink;

import com.mysql.jdbc.Driver;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * 从 1.13 开始，Flink JDBC sink 支持恰好一次模式。 该实现依赖于 XA 标准的 JDBC 驱动程序支持。
 *
 * 注意：在 1.13 中，Flink JDBC sink 不支持与 MySQL 或其他不支持每个连接多个 XA 事务的数据库的恰好一次模式。
 * 我们将改进 FLINK-22239 中的支持。
 *
 * 要使用它，请使用上述 exactOnceSink() 方法创建一个接收器，并额外提供：
 *
 * 仅一次选项
 * 执行选项
 * XA 数据源供应商
 *
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * env
 *         .fromElements(...)
 *         .addSink(JdbcSink.exactlyOnceSink(
 *                 "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
 *                 (ps, t) -> {
 *                     ps.setInt(1, t.id);
 *                     ps.setString(2, t.title);
 *                     ps.setString(3, t.author);
 *                     ps.setDouble(4, t.price);
 *                     ps.setInt(5, t.qty);
 *                 },
 *                 JdbcExecutionOptions.builder().build(),
 *                 JdbcExactlyOnceOptions.defaults(),
 *                 () -> {
 *                     // create a driver-specific XA DataSource
 *                     EmbeddedXADataSource ds = new EmbeddedXADataSource();
 *                     ds.setDatabaseName("my_db");
 *                     return ds;
 *                 });
 * env.execute();
 *
 * @author jilanyang
 * @date 2021/7/1 15:22
 * @packageName cn.jly.bigdata.flink.datastream.c04_sink
 * @className D01_JdbcSink
 */
public class D01_JdbcSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStreamSource<User> userDataStream = env.fromCollection(Arrays.asList(
                new User(null, "张三", 44),
                new User(null, "李四", 33),
                new User(null, "王五", 27)
        ));

        // sink
        userDataStream.addSink(
                JdbcSink.sink(
                        "insert into tbl_user(name, age) values(?, ?)",
                        new JdbcStatementBuilder<User>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, User user) throws SQLException {
                                preparedStatement.setString(1, user.name);
                                preparedStatement.setInt(2, user.age);
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(1000)
                                .withBatchSize(1000)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://linux01:3306/test")
                                .withDriverName(Driver.class.getName())
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )
        );

        env.execute("D01_JdbcSink");
    }

    public static class User {
        final Integer id;
        final String name;
        final Integer age;

        public User(Integer id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }
    }
}
