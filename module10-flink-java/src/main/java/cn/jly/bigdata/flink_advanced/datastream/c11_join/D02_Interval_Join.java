package cn.jly.bigdata.flink_advanced.datastream.c11_join;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * 前面的Window Join必须要在一个Window中进行JOIN，那如果没有Window如何处理呢？ interval join也是使用相同的key来join两个流（流A、流B），
 * 并且流B中的元素中的时间戳，和流A元素的时间戳，有一个时间间隔。 b.timestamp ∈ [a.timestamp + lowerBound; a.timestamp + upperBound] or a.timestamp +
 * lowerBound <= b.timestamp <= a.timestamp + upperBound 也就是： 流B的元素的时间戳 ≥ 流A的元素时间戳 + 下界，且，流B的元素的时间戳 ≤ 流A的元素时间戳 + 上界。
 *
 * @author jilanyang
 * @date 2021/8/8 22:16
 * @package cn.jly.bigdata.flink_advanced.datastream.c11_join
 * @class D02_Interval_Join
 */
public class D02_Interval_Join {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
    env.setParallelism(1);

    // 商品信息流
    DataStream<D01_Window_Join.Goods> goodsDS = env.addSource(new D01_Window_Join.GoodsSourceFunction())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<D01_Window_Join.Goods>() {
                  @Override
                  public WatermarkGenerator<D01_Window_Join.Goods> createWatermarkGenerator(Context context) {
                    return new WatermarkGenerator<D01_Window_Join.Goods>() {
                      @Override
                      public void onEvent(D01_Window_Join.Goods event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                      }

                      @Override
                      public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                      }
                    };
                  }
                })
                .withTimestampAssigner(new TimestampAssignerSupplier<D01_Window_Join.Goods>() {
                  @Override
                  public TimestampAssigner<D01_Window_Join.Goods> createTimestampAssigner(Context context) {
                    return new TimestampAssigner<D01_Window_Join.Goods>() {
                      @Override
                      public long extractTimestamp(D01_Window_Join.Goods element, long recordTimestamp) {
                        return System.currentTimeMillis();
                      }
                    };
                  }
                })
        );
    // 订单流
    DataStream<D01_Window_Join.OrderItem> orderItemDS = env.addSource(new D01_Window_Join.OrderItemSourceFunction())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<D01_Window_Join.OrderItem>() {
                  @Override
                  public WatermarkGenerator<D01_Window_Join.OrderItem> createWatermarkGenerator(Context context) {
                    return new WatermarkGenerator<D01_Window_Join.OrderItem>() {
                      @Override
                      public void onEvent(D01_Window_Join.OrderItem event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                      }

                      @Override
                      public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(System.currentTimeMillis()));
                      }
                    };
                  }
                })
                .withTimestampAssigner(new TimestampAssignerSupplier<D01_Window_Join.OrderItem>() {
                  @Override
                  public TimestampAssigner<D01_Window_Join.OrderItem> createTimestampAssigner(Context context) {
                    return new TimestampAssigner<D01_Window_Join.OrderItem>() {
                      @Override
                      public long extractTimestamp(D01_Window_Join.OrderItem element, long recordTimestamp) {
                        return System.currentTimeMillis();
                      }
                    };
                  }
                })
        );

    // interval join
    SingleOutputStreamOperator<D01_Window_Join.FactOrderItem> resDS =
        goodsDS.keyBy(g -> g.getGoodsId())
            // join的条件：
            // 1. goodsId要相等
            // 2. orderItemDS的时间戳 - 2 <= goodsDS的时间戳 <= orderItemDS的时间戳 + 1
            .intervalJoin(orderItemDS.keyBy(o -> o.getGoodsId()))
            .between(Time.seconds(-2), Time.seconds(1))
            .process(
                new ProcessJoinFunction<D01_Window_Join.Goods, D01_Window_Join.OrderItem, D01_Window_Join.FactOrderItem>() {
                  @Override
                  public void processElement(D01_Window_Join.Goods goods, D01_Window_Join.OrderItem orderItem,
                      Context ctx, Collector<D01_Window_Join.FactOrderItem> out) throws Exception {
                    D01_Window_Join.FactOrderItem factOrderItem = new D01_Window_Join.FactOrderItem(
                        goods.getGoodsId(),
                        goods.getGoodsName(),
                        BigDecimal.valueOf(orderItem.getCount()),
                        goods.getGoodsPrice().multiply(BigDecimal.valueOf(orderItem.getCount()))
                    );
                    out.collect(factOrderItem);
                  }
                });

    resDS.print();

    env.execute("D02_Interval_Join");
  }
}
