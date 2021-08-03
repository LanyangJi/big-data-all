package cn.jly.bigdata.flink_advanced.datastream.c05_connectors;

import cn.jly.bigdata.flink_advanced.datastream.beans.TblUser;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Flink的Kafka Producer  -  Flinkkafkaproducer允许将记录流写入一个或多个Kafka主题。
 * 构造函数接受以下参数：
 * 1 应该写入事件的默认输出主题
 * 2 SerializationsChema / KafkaserializationsChema，用于将数据序列化为Kafka
 * 3 KAFKA客户端的属性。需要以下属性： “bootstrap.servers”（逗号分隔的Kafka经纪人列表）
 * 4 一个容错语义
 * <p>
 * 场景：(实时ETL)
 * 实时接收socket数据（更多情况是kafka）, 经过实时的etl, 再写回到kafka
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c05_connectors
 * @class D03_Connectors_Kafka_Sink
 * @date 2021/7/27 22:14
 */
public class D03_Connectors_Kafka_Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> lineDs = env.socketTextStream("linux01", 9999);
        // 解析成TblUser类
        SingleOutputStreamOperator<TblUser> userDs = lineDs.map(new MapFunction<String, TblUser>() {
            @Override
            public TblUser map(String value) throws Exception {
                String[] fields = value.split(",");
                return new TblUser(Integer.parseInt(fields[0].trim()), fields[1].trim(), Integer.parseInt(fields[2].trim()));
            }
        });
        // 转换成json字符串
        SingleOutputStreamOperator<String> jsonDs = userDs.map(new MapFunction<TblUser, String>() {
            @Override
            public String map(TblUser value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        // 声明kafka sink
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("flink-test", new SimpleStringSchema(), properties);

        // sink to kafka
        jsonDs.addSink(kafkaProducer);

        env.execute("D03_Connectors_Kafka_Sink");
    }
}
