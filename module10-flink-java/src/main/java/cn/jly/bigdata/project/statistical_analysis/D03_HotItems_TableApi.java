package cn.jly.bigdata.project.statistical_analysis;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.or;

import cn.jly.bigdata.project.beans.UserBehavior;
import com.alibaba.fastjson.JSON;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author jilanyang
 * @date 2021/8/30 21:57
 */
public class D03_HotItems_TableApi {

  @SneakyThrows
  public static void main(String[] args) {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

    // 携带时间戳和水印的用户行为流
    SingleOutputStreamOperator<UserBehavior> userBehaviorDS = (SingleOutputStreamOperator<UserBehavior>) kafkaDS.map(
            new MapFunction<String, UserBehavior>() {
              @Override
              public UserBehavior map(String value) throws Exception {
                return JSON.parseObject(value, UserBehavior.class);
              }
            }
        )
        .assignTimestampsAndWatermarks(
            // 给升序数据指定时间戳和水印
            WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<UserBehavior>() {
                      @Override
                      public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp();
                      }
                    }
                )
        );

    // 创建表格
        /*
             private Long userId;
            private Long itemId;
            private Integer categoryId;
            private String behavior;
            private Long timestamp;
         */
    Table ubTable = tableEnv.fromDataStream(
        userBehaviorDS,
        $("userId"),
        $("itemId"),
        $("categoryId"),
        $("behavior"),
        $("timestamp").rowtime().as("ts")
    );

    // table api
    Table aggTable = ubTable.filter(
            or(
                $("behavior").isEqual("pv"),
                $("behavior").isEqual("PV")
            )
        )
        .window(
            // 每5秒统计过去10s
            Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("ts")).as("w")
        )
        .groupBy($("w"), $("itemId"))
        .select(
            $("itemId"),
            $("behavior").count().as("cnt"),
            $("w").end().as("windowEnd")
        );

    // 打印输出
//        DataStream<Tuple2<Boolean, Row>> resDS = tableEnv.toRetractStream(aggTable, Row.class);
//        resDS.print();

    // 利用开窗函数，对count进行排序并获取row number,得到topN
    tableEnv.createTemporaryView("aggTable", aggTable);
    Table resTable = tableEnv.sqlQuery(
        "select * from (" +
            "    select *, row_number() over(partition by windowEnd order by cnt desc) as row_num from aggTable" +
            ") t where t.row_num <= 5"
    );

    // 打印输出
    // tableEnv.toRetractStream(resTable, Row.class).print();

    // todo 纯sql搞定？
    tableEnv.createTemporaryView("ubTable", ubTable);
    Table resTable2 = tableEnv.sqlQuery(
        "select * " +
            "from ( " +
            "     select  " +
            "          *, " +
            "          row_number() over(partition by windowEnd order by cnt desc) as row_num " +
            "     from ( " +
            "          select  " +
            "               itemId, " +
            "               count(userId) as cnt, " +
            "               hop_end(ts, interval '10' seconds, interval '5' seconds) as windowEnd " +
            "          from ubTable " +
            "          group by itemId, hop(ts, interval '10' seconds, interval '5' seconds) " +
            "     ) " +
            ") t  " +
            "where t.row_num <= 5"
    );

    tableEnv.toRetractStream(resTable2, Row.class).printToErr();

    env.execute("D03_HotItems_TableApi");
  }
}
