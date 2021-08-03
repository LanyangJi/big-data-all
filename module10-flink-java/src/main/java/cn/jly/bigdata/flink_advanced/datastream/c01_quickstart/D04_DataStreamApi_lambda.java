package cn.jly.bigdata.flink_advanced.datastream.c01_quickstart;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * lambda的方式
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c01_quickstart
 * @class D04_DataStreamApi_lambda
 * @date 2021/7/24 23:00
 */
public class D04_DataStreamApi_lambda {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 执行处理模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        /*
        缺少“收集器”的通用类型参数。在许多情况下，当涉及 Java 泛型时，lambda 方法无法为自动类型提取提供足够的信息。
        一个简单的解决方法是使用（匿名）类来实现“org.apache.flink.api.common.functions.FlatMapFunction”接口。
        否则必须使用类型信息显式指定类型。

        简而言之就是lambda方式会存在泛型类型擦除的问题，导致flink无法准确识别返回值类型
        解决方法：1. 使用匿名子类的方式
                2. 使用returns方法明确指定返回值
         */
        SingleOutputStreamOperator<String> wordDS = lineDS.flatMap(
                (String line, Collector<String> collector) ->
                        Arrays.stream(line.split(" ")).forEach(collector::collect)
        ).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = wordDS.map(
                (String word) -> Tuple2.of(word, 1L)
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyedDS = wordAndOneDS.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);

        sumDS.print();

        // 执行
        env.execute("D04_DataStreamApi_lambda");
    }
}
