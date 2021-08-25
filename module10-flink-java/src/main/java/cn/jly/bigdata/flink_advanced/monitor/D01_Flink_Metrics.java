package cn.jly.bigdata.flink_advanced.monitor;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink监控与优化篇
 * Metrics 的类型如下：
 * 1. 常用的如 Counter，写过 mapreduce 作业的开发人员就应该很熟悉 Counter，其实含义都是一样的，就是对一个计数器进行累加，即对于多条数据和多兆数据一直往上加的过程。
 * 2. Gauge，Gauge 是最简单的 Metrics，它反映一个值。比如要看现在 Java heap 内存用了多少，就可以每次实时的暴露一个 Gauge，Gauge 当前的值就是heap使用的量。
 * 3. Meter，Meter 是指统计吞吐量和单位时间内发生“事件”的次数。它相当于求一种速率，即事件次数除以使用的时间。
 * 4. Histogram，Histogram 比较复杂，也并不常用，Histogram 用于统计一些数据的分布，比如说 Quantile、Mean、StdDev、Max、Min 等。
 * Metric 在 Flink 内部有多层结构，以 Group 的方式组织，它并不是一个扁平化的结构，Metric Group + Metric Name 是 Metrics 的唯一标识。
 * <p>
 * 案例：自定义监控指标
 * 在map算子内统计输入的数据条目
 * - MetricGroup为flink_test_metric
 * - 指标变量为mapDataNum
 *
 * 提交到flink集群上跑，通过flink的WebUI查看监控指标
 *
 * @author jilanyang
 * @date 2021/8/24 20:49
 */
public class D01_Flink_Metrics {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.socketTextStream("localhost", 9999)
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String value, Collector<String> out) throws Exception {
                                for (String s : value.split(" ")) {
                                    out.collect(s);
                                }
                            }
                        }
                )
                .map(
                        new RichMapFunction<String, Tuple2<String, Integer>>() {

                            private Counter counter;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                counter = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("mapDataNum");
                            }

                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                // 每来一条数据，counter就加1
                                counter.inc();
                                return Tuple2.of(value, 1);
                            }
                        }
                )
                .keyBy(
                        t -> t.f0
                )
                .sum(1)
                .print();

        env.execute("D01_Flink_Metrics");
    }
}
