package cn.jly.bigdata.flink.datastream.c04_sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Apache Bahir提供了flink-connector-redis_2.11用以sink到redis，但是比较老
 * 这边自己基于lettuce实现source和sink
 *
 * @author lanyangji
 * @date 2021/7/2 15:54
 * @packageName cn.jly.bigdata.flink.datastream.c04_sink
 * @className D03_RedisSink
 */
public class D03_RedisSink {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            if (StringUtils.isNotBlank(word)) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                    }
                })
                // .returns(Types.TUPLE(Types.STRING, Types.INT)) // lambda表达式形式明确指定返回值类型
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Tuple2<String, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1.toString());
                    }
                })
                .addSink(new RedisSink(host, port, "word_count"));


        env.execute("D03_RedisSink");
    }

    /**
     * 自定义sinkFunction，把数据sink到redis中
     */
    public static class RedisSink extends RichSinkFunction<Tuple2<String, String>> {
        private final String host;
        private final int port;
        private final String key;

        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connection;

        public RedisSink(String host, int port, String key) {
            this.host = host;
            this.port = port;
            this.key = key;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.redisClient = RedisClient.create(new RedisURI(this.host, this.port, Duration.ofSeconds(60)));
            this.connection = redisClient.connect();
        }

        @Override
        public void close() throws Exception {
            if (null != this.connection) {
                this.connection.close();
            }

            if (null != this.redisClient) {
                this.redisClient.shutdown();
            }
        }

        @Override
        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            RedisAsyncCommands<String, String> commands = this.connection.async();
            commands.hset(this.key, value.f0, value.f1);
        }
    }
}
