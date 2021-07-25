package cn.jly.bigdata.flink_advanced.datastream.c02_source;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

/**
 * 自定义数据源
 * <p>
 * Flink还提供了数据源接口,我们实现该接口就可以实现自定义数据源，不同的接口有不同的功能，分类如下：
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c02_source
 * @class D04_CustomSource
 * @date 2021/7/25 17:44
 */
public class D04_CustomSource {
    public static void main(String[] args) throws Exception {
        // 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 由于这边实现的是支持并行化的数据源，因此在不设置并行度的情况下，默认是按照本机最大线程数作为并行度
        DataStreamSource<Order> orderDS = env.addSource(new MyOrderSource());

        // 打印
        orderDS.print();

        env.execute("D04_CustomSource");
    }

    /**
     * 自定义支持并行化的richSourceFunction
     * <p>
     * 测试案例：每隔一秒生成一条订单信息
     */
    public static class MyOrderSource extends RichParallelSourceFunction<Order> {
        private Boolean flag = Boolean.TRUE;

        /**
         * 执行并生成数据（run方法只执行一次，所以如果需要源源不断的数据，需要通过循环等方法）
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {
            Random random = new Random();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            // double值只保留2位小数
            NumberFormat numberFormat = NumberFormat.getNumberInstance();
            numberFormat.setMaximumFractionDigits(2);

            while (flag) {
                // 订单id
                String orderId = UUID.randomUUID().toString();
                // userId，范围0-2
                int userId = random.nextInt(3);
                double money = Double.parseDouble(numberFormat.format(random.nextDouble() * 100d));

                String createTime = dateTimeFormatter.format(LocalDateTime.now());

                // 写出去
                sourceContext.collect(Order.builder().id(orderId).userId(userId).money(money).createTime(createTime).build());

                // 每隔一秒
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Order {
        private String id;
        private Integer userId;
        private double money;
        private String createTime;
    }
}
