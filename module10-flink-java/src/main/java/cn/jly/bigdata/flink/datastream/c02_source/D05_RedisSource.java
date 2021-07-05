package cn.jly.bigdata.flink.datastream.c02_source;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.Duration;
import java.util.Map;

/**
 * Apache Bahir提供了flink-connector-redis_2.11用以sink到redis，但是比较老
 * 这边自己基于lettuce实现source和sink
 *
 * @author lanyangji
 * @date 2021/7/2 15:13
 * @packageName cn.jly.bigdata.flink.datastream.c02_source
 * @className D05_RedisSource
 */
public class D05_RedisSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, String>> dataStream =
                env.addSource(new RedisSourceFunction("linux01", 6379, "flink-test"));

        dataStream.print();

        env.execute("D05_RedisSource");
    }

    public static class RedisSourceFunction extends RichSourceFunction<Tuple2<String, String>> {
        RedisClient redisClient = null;
        StatefulRedisConnection<String, String> connection = null;

        private final String host;
        private final int port;
        private final String hashKey;

        public RedisSourceFunction(String host, int port, String hashKey) {
            this.hashKey = hashKey;
            this.host = host;
            this.port = port;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.redisClient = RedisClient.create(new RedisURI(host, port, Duration.ofSeconds(60)));
            this.connection = this.redisClient.connect();
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            RedisCommands<String, String> commands = this.connection.sync();
            Map<String, String> map = commands.hgetall(hashKey);
            if (!map.isEmpty()) {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    ctx.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                }
            }
        }

        @Override
        public void cancel() {
            this.connection.close();
            this.redisClient.shutdown();
        }
    }
}
