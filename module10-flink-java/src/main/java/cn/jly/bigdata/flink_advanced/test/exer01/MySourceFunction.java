package cn.jly.bigdata.flink_advanced.test.exer01;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * 自定义数据源
 * <p>
 * 首先我们通过自定义source 模拟订单的生成，生成了一个Tuple2,第一个元素是分类，第二个元素表示这个分类下产生的订单金额，金额我们通过随机生成
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class MySourceFunction
 * @date 2021/8/4 21:10
 */
public class MySourceFunction extends RichParallelSourceFunction<Tuple2<String, Double>> {
    private boolean flag = true;
    private final String[] categories = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};
    private final Random random = new Random();

    @Override
    public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
        while (flag) {
            int index = random.nextInt(categories.length);
            String category = categories[index];
            // [0, 1) * 100, 且保留两位小数
            Double price = BigDecimal.valueOf(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_UP).doubleValue();
            ctx.collect(Tuple2.of(category, price));

            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
