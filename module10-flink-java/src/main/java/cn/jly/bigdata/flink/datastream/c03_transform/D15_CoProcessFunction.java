package cn.jly.bigdata.flink.datastream.c03_transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lanyangji
 * @date 2021/7/7 11:17
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D15_CoProcessFunction
 */
public class D15_CoProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> ds2 = env.fromElements("tom", "amy", "kobe");

        ds1.connect(ds2)
                .process(new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                    }
                })
                .printToErr();

        env.execute("D15_CoProcessFunction");
    }
}
