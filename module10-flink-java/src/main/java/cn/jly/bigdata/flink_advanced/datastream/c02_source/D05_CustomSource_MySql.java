package cn.jly.bigdata.flink_advanced.datastream.c02_source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 实际开发中,经常会实时接收一些数据,要和MySQL中存储的一些规则进行匹配,那么这时候就可以使用Flink自定义数据源从MySQL中读取数据
 * 那么现在先完成一个简单的需求:
 * 从MySQL中实时加载数据
 * 要求MySQL中的数据有变化,也能被实时加载出来
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c02_source
 * @class D05_CustomSource_MySql
 * @date 2021/7/25 20:29
 */
public class D05_CustomSource_MySql {
    public static void main(String[] args) throws Exception {

        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // mysql source
        env.addSource(new MysqlSource("jdbc:mysql://linux01:3306/test", "root", "123456")).setParallelism(1)
                .print();

        env.execute("D05_CustomSource_MySql");
    }

    /**
     * 自定义数据源实时读取数据库中的数据
     * 要求MySQL中的数据有变化,也能被实时加载出来
     */
    public static class MysqlSource extends RichParallelSourceFunction<TblUser> {
        private Boolean flag = true;
        private Connection connection = null;
        private PreparedStatement preparedStatement = null;
        private ResultSet resultSet = null;

        private final String jdbcUrl;
        private final String user;
        private final String password;

        public MysqlSource(String jdbcUrl, String user, String password) {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
        }

        /**
         * 初始化时执行一次
         * 适合用来开启资源
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            String querySql = "select id, name, age from tbl_user";
            preparedStatement = connection.prepareStatement(querySql);
        }

        /**
         * 启动时执行一次，加载数据
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext<TblUser> sourceContext) throws Exception {
            while (flag) {
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int age = resultSet.getInt("age");

                    sourceContext.collect(new TblUser(id, name, age));
                }

                // 每5秒访问一次
                Thread.sleep(5000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TblUser {
        private Integer id;
        private String name;
        private Integer age;
    }
}
