package cn.jly.bigdata.flink.datastream.c04_sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author lanyangji
 * @date 2021/7/2 14:28
 * @packageName cn.jly.bigdata.flink.datastream.c04_sink
 * @className D02_KafkaSink
 */
public class D02_KafkaSink {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "flink-test",
                new SimpleStringSchema(),
                properties
        );

        env.socketTextStream(host, port)
                .addSink(kafkaProducer);

        env.execute("D02_KafkaSink");
    }
}
