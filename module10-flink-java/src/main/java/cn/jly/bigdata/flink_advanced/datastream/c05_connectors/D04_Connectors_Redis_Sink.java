package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import cn.jly.bigdata.flink_advanced.datastream.beans.TblUser;
import com.alibaba.fastjson.JSON;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * redis连接
 * <p>
 * flink官网也说https://bahir.apache.org/docs/flink/current/flink-streaming-redis/提供了连接器，但是版本太老
 * 这里不采用，而是采用自己自定义sink的方式实现
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @class D04_Connectors_Redis
 * @date 2021/7/28 20:28
 */
public class D04_Connectors_Redis_Sink {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.socketTextStream(host, 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                        TblUser user = JSON.parseObject(value, TblUser.class);
                        out.collect(Tuple2.of(user.getName(), value));
                    }
                })
                .addSink(new LettuceRedisSink(host, 6379, "tbl_user"));

        env.execute("D04_Connectors_Redis_Sink");
    }

    /**
     * 自定义redis sink
     */
    public static class LettuceRedisSink extends RichSinkFunction<Tuple2<String, String>> {
        private final String host;
        private final Integer port;
        /**
         * Key
         */
        private final String key;
        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connection;

        /**
         * @param host redis服务端主机ip
         * @param port redis端口号
         * @param key  hash key
         */
        public LettuceRedisSink(String host, Integer port, String key) {
            this.host = host;
            this.port = port;
            this.key = key;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创立客户端，超时时间是10秒钟
            redisClient = RedisClient.create(new RedisURI(host, port, Duration.of(10, ChronoUnit.SECONDS)));
            connection = redisClient.connect();
        }

        @Override
        public void close() throws Exception {
            if (null != connection)
                connection.close();

            if (null != redisClient)
                redisClient.shutdown();
        }

        /**
         * 写入
         *
         * @param tuple
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Tuple2<String, String> tuple, Context context) throws Exception {
            String hashKey = tuple.f0;
            String value = tuple.f1;

            RedisAsyncCommands<String, String> commands = connection.async();
            commands.hset(key, hashKey, value);
        }
    }
}
