package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.math3.geometry.enclosing.EnclosingBall;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @class D04_Connectors_Redis_Source
 * @date 2021/7/28 22:32
 */
public class D04_Connectors_Redis_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Tuple2<String, String>> redisDs = env.addSource(new LettuceRedisSource("linux01", 6379, "tbl_user"));
        redisDs.print();

        env.execute("D04_Connectors_Redis_Source");
    }

    public static class LettuceRedisSource extends RichSourceFunction<Tuple2<String, String>> {
        private final String host;
        private final Integer port;
        /**
         * Key
         */
        private final String key;
        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connection;
        private boolean isRunning = true;

        /**
         * @param host redis服务端主机ip
         * @param port redis端口号
         * @param key  hash key
         */
        public LettuceRedisSource(String host, Integer port, String key) {
            this.host = host;
            this.port = port;
            this.key = key;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 超时时间10秒
            redisClient = RedisClient.create(new RedisURI(host, port, Duration.of(10, ChronoUnit.SECONDS)));
            connection = redisClient.connect();
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
            // 一次性加载所有
            if (isRunning) {
                RedisCommands<String, String> commands = connection.sync();
                Map<String, String> map = commands.hgetall(key);
                if (MapUtils.isNotEmpty(map)) {
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        sourceContext.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (null != connection)
                connection.close();
            if (null != redisClient)
                redisClient.shutdown();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
