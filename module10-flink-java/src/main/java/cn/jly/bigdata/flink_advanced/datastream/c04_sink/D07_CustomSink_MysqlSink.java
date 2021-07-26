package cn.jly.bigdata.flink_advanced.datastream.c04_sink;

import cn.jly.bigdata.flink_advanced.datastream.beans.TblUser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author jilanyang
 * @date 2021/7/26 16:35
 * @package cn.jly.bigdata.flink_advanced.datastream.c04_sink
 * @class D07_CustomSink_MysqlSink
 */
public class D07_CustomSink_MysqlSink {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        env.execute("D07_CustomSink_MysqlSink");
    }

    /**
     * 自定义sink function
     */
    public static class MysqlSink extends RichSinkFunction<TblUser> {

        private String jdbcUrl;
        private String user;
        private String password;

        private Connection connection;
        private PreparedStatement preparedStatement;

        public MysqlSink(String jdbcUrl, String user, String password) {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(jdbcUrl,user, password);

        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public void invoke(TblUser value, Context context) throws Exception {
        }
    }
}
