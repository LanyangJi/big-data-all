package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

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
 * 注意：！！！！！！！！！！！！！
 *      另外还有一种基础传统的source function实现kafka source的方式，
 *      在{@link D02_Connectors_Kafka_SourceFunction}中
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
        /*
            会开启一个后台线程每隔5秒检测一下kafka的分区情况，实现动态分区监测
            为了在不重新启动Flink作业的情况下处理主题扩展或主题创建等场景，
            可以将Kafka源配置为在提供的主题分区订阅模式下定期发现新分区。
            要启用分区发现，请为partition.discovery.interval.ms属性设置一个非负值：
            默认情况下禁用分区发现。您需要显式设置分区发现间隔以启用此功能。
         */
        properties.setProperty("partition.discovery.interval.ms", "5000");
        /*
         开启自动提交（提交到默认主题中__consumer_offsets，后续学习了checkpoint后，会随着checkpoint存储在checkpoint和默认主题中）
         Kafka source在检查点完成时提交当前消耗的偏移量，以确保Flink的检查点状态与Kafka代理上提交的偏移量之间的一致性。
            如果未启用检查点，则Kafka源依赖于Kafka使用者的内部自动定期偏移提交逻辑，该逻辑由Kafka使用者属性中的enable.auto.commit和auto.commit.interval.ms配置。
            注意，Kafka源代码不依赖于提交的偏移量来实现容错。提交偏移量仅用于公开消费者和消费组的进度以进行监视。
         */
        properties.setProperty("enable.auto.commit", "true");
        // 自动提交时间间隔
        properties.setProperty("auto.commit.interval.ms", "2000");

        // 声明kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                // .setTopicPattern(Pattern.compile("connectors-topic*"))
                .setTopics("flink-test")
                .setGroupId(UUID.randomUUID().toString())
                // 没设置的话，earliest也是默认值
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        /*
            默认情况下，记录将使用Kafka ConsumerRecord中嵌入的时间戳作为事件时间。
            您可以定义自己的水印策略，从记录本身提取事件时间，并向下游发出水印：
            env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy")

            文档：
            https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/event-time/generating_watermarks/
         */
        DataStreamSource<String> kafkaDs
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        kafkaDs.print();

        env.execute("D02_Connectors_Kafka_Source");
    }
}
