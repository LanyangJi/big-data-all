package cn.jly.bigdata.flink_advanced.datastream.c03_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流：
 * 1.11版本及之前有split+select方式的分流。
 * 1.12版本之后直接移除了这两个api，采用process+sideOutput方式来处理
 * <p>
 * 示例：奇偶数拆分
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c03_transformation
 * @class D03_SideOutput
 * @date 2021/7/25 22:02
 */
public class D03_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> sequenceDS = env.fromSequence(1, 20);

        // 正常输出偶数，奇数通过侧输出流输出
        OutputTag<Long> oddOutputTag = new OutputTag<Long>("odd_output_tag", TypeInformation.of(Long.class)) {
        };

        SingleOutputStreamOperator<Long> processDS = sequenceDS.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, ProcessFunction<Long, Long>.Context context, Collector<Long> collector) throws Exception {
                if (value % 2 == 0) {
                    collector.collect(value);
                } else {
                    context.output(oddOutputTag, value);
                }
            }
        });

        // 正常输出
        processDS.print("even");

        // 获取侧输出流并输出
        DataStream<Long> sideOutputDS = processDS.getSideOutput(oddOutputTag);
        sideOutputDS.printToErr("odd");

        env.execute("D03_SideOutput");
    }
}
