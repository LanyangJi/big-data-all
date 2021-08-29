package cn.jly.bigdata.project.statistical_analysis;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author jilanyang
 * @date 2021/8/29 21:26
 */
public class D02_HotItems_Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 1. 读取数据 - 带时间戳和水印的用户行为数据流
        // DataStreamSource<String> userBehaviorDS = env.readTextFile("D:\\JLY\\test\\Data\\UserBehavior.csv");

        // 2. kafka数据源
        Properties properties = new Properties();
        // Kafka集群地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        // 消费者id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动重置偏移量，记录了偏移量就从偏移量位置读，否则重置为配置的最新
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 允许自动提交
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // kv反序列化
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FlinkKafkaConsumer<String> kafkaSourceFunction = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties
        );
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSourceFunction);

        // 打印输出
        kafkaDS.print();

        env.execute("D02_HotItems_Kafka");
    }
}
