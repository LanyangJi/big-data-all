package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.UUID;

/**
 * Flink官方连接器之Kafka
 * Apache Flink 附带了一个通用的 Kafka 连接器，它试图跟踪最新版本的 Kafka 客户端。
 * 它使用的客户端版本可能会在 Flink 版本之间发生变化。
 * 现代 Kafka 客户端向后兼容代理版本 0.10.0 或更高版本。关于 Kafka 兼容性的详细信息，请参考 Kafka 官方文档。
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-kafka_2.11</artifactId>
 * <version>1.13.0</version>
 * </dependency>
 * <p>
 * 如果您使用的是 Kafka 源，还需要 flink-connector-base 作为依赖项：
 *
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-base</artifactId>
 * <version>1.13.0</version>
 * </dependency>
 * <p>
 * Kafka 源提供了一个构建器类来构建 KafkaSource 的实例。
 * 下面的代码片段展示了如何构建一个 KafkaSource 来消费来自主题“input-topic”最早偏移量的消息，
 * 消费者组是“my-group”，并且仅将消息的值反序列化为字符串。
 * <p>
 * 构建 KafkaSource 需要以下属性：
 * 1 kafka服务器，由 setBootstrapServers(String) 配置
 * 2 消费者组 ID，由 setGroupId(String) 配置
 * 3 要订阅的主题/分区，请参阅以下主题-分区订阅以了解更多详细信息。
 * 4 用于解析 Kafka 消息的 Deserializer，更多详细信息请参见下面的 Deserializer。
 * <p>
 * 主题分区订阅#
 * Kafka源码提供了3种topic-partition订阅方式：
 * 1. 主题列表，订阅主题列表中所有分区的消息。例如：
 * KafkaSource.builder().setTopics("topic-a", "topic-b")
 *
 * @author jilanyang
 * @date 2021/7/27 16:45
 * @package cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @class D02_Connectors_Kafka_Source
 */
public class D02_Connectors_Kafka_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        String bootstrapServers = "linux01:9092";
        // 配置
        Properties properties = new Properties();
        // 有offset则从offset中开始消费，没有则从最新的位置开始消费；earliest有offset从记录的位置开始消费，没有则从最开始的位置消费
        // properties.setProperty("auto.offset.reset", "latest");
        // 会开启一个后台线程每隔5秒检测一下kafka的分区情况，实现动态分区监测
        properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        // 开启自动提交（提交到默认主题中__consumer_offsets，后续学习了checkpoint后，会随着checkpoint存储在checkpoint和默认主题中）
        properties.setProperty("enable.auto.commit", "true");
        // 自动提交时间间隔
        properties.setProperty("auto.commit.interval.ms", "2000");

        // 声明kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                // .setTopicPattern(Pattern.compile("connectors-topic*"))
                .setTopics("flink-test")
                .setGroupId(UUID.randomUUID().toString())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        DataStreamSource<String> kafkaDs
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        kafkaDs.print();

        env.execute("D02_Connectors_Kafka_Source");
    }
}
