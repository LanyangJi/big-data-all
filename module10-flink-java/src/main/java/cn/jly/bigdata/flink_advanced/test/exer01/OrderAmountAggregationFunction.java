package cn.jly.bigdata.flink_advanced.test.exer01;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义订单金额聚合函数
 * 1. 输入为商品品类和成交金额的二元组
 * 2. 累加器就是要聚合的成交金额，double
 * 3. 返回值就是成交金额的聚合结果
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class OrderAmountAggregationFunction
 * @date 2021/8/4 21:46
 */
public class OrderAmountAggregationFunction implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
    @Override
    public Double createAccumulator() {
        return 0d;
    }

    /**
     * 将数据累加到累加器上
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Double add(Tuple2<String, Double> value, Double accumulator) {
        return value.f1 + accumulator;
    }

    /**
     * 获取累加结果
     *
     * @param accumulator
     * @return
     */
    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    /**
     * 合并各个subTask的累加结果
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
