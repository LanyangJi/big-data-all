package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * <dependency>
 *      <groupId>org.apache.flink</groupId>
 *      <artifactId>flink-connector-kafka_2.12</artifactId>
 *      <version>${flink.version}</version>
 * </dependency>
 *
 * @author jilanyang
 * @date 2021/7/1 14:25
 * @packageName cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @className D02_Connectors_Kafka_SourceFunction
 */
public class D02_Connectors_Kafka_SourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "lanyangji");
        /*
         FlinkKafkaConsumer
         topic支持正则表达式匹配
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("test-topic-[0-9]"),
                new SimpleStringSchema(),
                properties);
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                new FlinkKafkaConsumer<>("flink-test", new SimpleStringSchema(), properties);
        // 从最早位置开始消费
        flinkKafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> kafkaDataStream = env.addSource(flinkKafkaConsumer);
        kafkaDataStream.print();

        env.execute("D02_Connectors_Kafka_SourceFunction");
    }
}
