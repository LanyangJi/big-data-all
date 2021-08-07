package cn.jly.bigdata.flink_advanced.test.exer01;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 自定义窗口函数，指定窗口数据收集规则
 * 品类订单金额聚合窗口函数
 * <p>
 * Type parameters:
 * <Double> – The type of the input value.
 * <CategoryPojo> – The type of the output value.
 * <String> – The type of the key.
 * <TimeWindow – The type of Window that this window function can be applied on.
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class OrderAmountWindowFunction
 * @date 2021/8/4 21:56
 */
public class OrderAmountWindowFunction implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
    private final FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    @Override
    public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
        double totalPrice = 0D;
        for (Double price : input) {
            // 保留两位小数
            double newPrice = BigDecimal.valueOf(price).setScale(2, RoundingMode.HALF_UP).doubleValue();
            totalPrice += newPrice;
        }
        out.collect(new CategoryPojo(key, totalPrice, fastDateFormat.format(System.currentTimeMillis())));
    }
}
