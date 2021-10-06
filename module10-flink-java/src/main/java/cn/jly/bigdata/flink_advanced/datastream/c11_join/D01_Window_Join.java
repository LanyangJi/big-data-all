package cn.jly.bigdata.flink_advanced.datastream.c11_join;

import com.alibaba.fastjson.JSON;
import com.sun.corba.se.impl.oa.toa.TransientObjectManager;
import jdk.internal.dynalink.linker.LinkerServices;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.omg.CORBA.PRIVATE_MEMBER;

import javax.print.DocFlavor;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 双流Join是Flink面试的高频问题。一般情况下说明以下几点就可以hold了： Join大体分类只有两种：Window Join和Interval Join。 1. Window Join又可以根据Window的类型细分出3种： Tumbling Window
 * Join、Sliding Window Join、Session Windnow Join。 Windows类型的join都是利用window的机制，先将数据缓存在Window State中，当窗口触发计算时，执行join操作； 2. interval
 * join也是利用state存储数据再处理，区别在于state中的数据有失效机制，依靠数据触发数据清理； 目前Stream join的结果是数据的笛卡尔积；
 * <p>
 * 关于语义的一些注意事项： 创建两个流的元素的成对组合的行为类似于内连接，这意味着如果一个流中的元素没有来自另一个流的相应元素要连接，则不会发出它们。 那些确实被加入的元素将把仍然位于各自窗口中的最大时间戳作为它们的时间戳。例如，以 [5, 10)
 * 为边界的窗口将导致连接元素的时间戳为 9。
 *
 * @author jilanyang
 * @date 2021/8/8 19:29
 * @package cn.jly.bigdata.flink_advanced.datastream.c11_join
 * @class D01_Window_Join
 */
public class D01_Window_Join {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
    env.setParallelism(1);

    // 商品信息流
    DataStream<Goods> goodsDS = env.addSource(new GoodsSourceFunction())
        .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<Goods>() {
          @Override
          public WatermarkGenerator<Goods> createWatermarkGenerator(Context context) {
            return new WatermarkGenerator<Goods>() {
              @Override
              public void onEvent(Goods event, long eventTimestamp, WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
              }

              @Override
              public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
              }
            };
          }
        }).withTimestampAssigner(new TimestampAssignerSupplier<Goods>() {
          @Override
          public TimestampAssigner<Goods> createTimestampAssigner(Context context) {
            return new TimestampAssigner<Goods>() {
              @Override
              public long extractTimestamp(Goods element, long recordTimestamp) {
                return System.currentTimeMillis();
              }
            };
          }
        }));
    // 订单流
    DataStream<OrderItem> orderItemDS = env.addSource(new OrderItemSourceFunction())
        .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<OrderItem>() {
          @Override
          public WatermarkGenerator<OrderItem> createWatermarkGenerator(Context context) {
            return new WatermarkGenerator<OrderItem>() {
              @Override
              public void onEvent(OrderItem event, long eventTimestamp, WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
              }

              @Override
              public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
              }
            };
          }
        }).withTimestampAssigner(new TimestampAssignerSupplier<OrderItem>() {
          @Override
          public TimestampAssigner<OrderItem> createTimestampAssigner(Context context) {
            return new TimestampAssigner<OrderItem>() {
              @Override
              public long extractTimestamp(OrderItem element, long recordTimestamp) {
                return System.currentTimeMillis();
              }
            };
          }
        }));

    // join
    DataStream<FactOrderItem> resDS = goodsDS.join(orderItemDS)
        // where和equalTo联合指定join条件
        .where(new KeySelector<Goods, String>() {
          @Override
          public String getKey(Goods goods) throws Exception {
            return goods.getGoodsId();
          }
        }).equalTo(new KeySelector<OrderItem, String>() {
          @Override
          public String getKey(OrderItem orderItem) throws Exception {
            return orderItem.getGoodsId();
          }
        })
        // 开窗处理,这边以滚动窗口为例
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new JoinFunction<Goods, OrderItem, FactOrderItem>() {
          @Override
          public FactOrderItem join(Goods goods, OrderItem orderItem) throws Exception {
            return new FactOrderItem(goods.getGoodsId(), goods.getGoodsName(),
                BigDecimal.valueOf(orderItem.getCount()),
                goods.getGoodsPrice().multiply(BigDecimal.valueOf(orderItem.getCount())));
          }
        });

    resDS.print();

    env.execute("D01_Window_Join");
  }

  public static class OrderItemSourceFunction extends RichParallelSourceFunction<OrderItem> {

    private boolean flag = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<OrderItem> ctx) throws Exception {
      while (flag) {
        Goods goods = Goods.randomGoods();
        OrderItem orderItem = new OrderItem();
        orderItem.setGoodsId(goods.getGoodsId());
        orderItem.setCount(random.nextInt(10) + 1);
        orderItem.setItemId(UUID.randomUUID().toString());
        ctx.collect(orderItem);
        orderItem.setGoodsId("111");
        ctx.collect(orderItem); // 每次收集2条数据,模拟一个订单有多个商品
        TimeUnit.MILLISECONDS.sleep(500);
      }
    }

    @Override
    public void cancel() {
      this.flag = false;
    }
  }

  /**
   * 构建一个商品Stream源（这个就好比维表）
   */
  public static class GoodsSourceFunction extends RichParallelSourceFunction<Goods> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<Goods> ctx) throws Exception {
      while (flag) {
        Goods.GOODS_LIST.forEach(ctx::collect);

        TimeUnit.MILLISECONDS.sleep(500);
      }
    }

    @Override
    public void cancel() {
      this.flag = false;
    }
  }

  /**
   * 关联结果
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class FactOrderItem {

    private String goodsId;
    private String goodsName;
    private BigDecimal count;
    private BigDecimal totalMoney;

    @Override
    public String toString() {
      return JSON.toJSONString(this);
    }
  }

  /**
   * 订单明细类
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class OrderItem {

    private String itemId;
    private String goodsId;
    private Integer count;

    @Override
    public String toString() {
      return JSON.toJSONString(this);
    }
  }

  /**
   * 商品明细类
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Goods {

    private String goodsId;
    private String goodsName;
    private BigDecimal goodsPrice;
    private static List<Goods> GOODS_LIST;
    private static Random random;

    static {
      random = new Random();
      GOODS_LIST = new ArrayList<>();

      GOODS_LIST.add(new Goods("1", "小米", new BigDecimal(4890)));
      GOODS_LIST.add(new Goods("2", "iphone12", new BigDecimal(12000)));
      GOODS_LIST.add(new Goods("3", "MacBookPro", new BigDecimal(15000)));
      GOODS_LIST.add(new Goods("4", "ThinkPad X1", new BigDecimal(9800)));
      GOODS_LIST.add(new Goods("5", "MeiZu One", new BigDecimal(3200)));
      GOODS_LIST.add(new Goods("6", "HuaWei Mate 40", new BigDecimal(6500)));
    }

    public static Goods randomGoods() {
      return GOODS_LIST.get(random.nextInt(GOODS_LIST.size()));
    }
  }
}
