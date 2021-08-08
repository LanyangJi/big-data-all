package cn.jly.bigdata.flink_advanced.test.exer02;

import cn.hutool.core.util.RandomUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Objects;

/**
 * 订单自动好评练习
 * <p>
 * 一般订单完成之后，如果用户在一定时间没有评价，系统自动给予五星好评
 *
 * @author jilanyang
 * @date 2021/8/7 21:39
 * @package cn.jly.bigdata.flink_advanced.test.exer02
 * @class D01_Order_Auto_Favorable
 */
public class D01_Order_Auto_Favorable_Main_App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 为了方便查看,并行度设置为1
        env.setParallelism(1);

        // 经过 interval 时间用户未给出评价的订单,自动给出好评,实际中应该是多少天之后，这里为了测试方便, interval 设置为5秒
        // source - <用户id,订单id,订单生成时间>
        DataStreamSource<Tuple3<String, String, Long>> orderDS = env.addSource(new MySourceFunction());
        long interval = 5000L; // 5秒
        orderDS.keyBy(t -> t.f0)
                .process(new AutoFavorableKeyedProcessFunction(interval))
                .printToErr();


        env.execute("D01_Order_Auto_Favorable_Main_App");
    }

    /**
     * 订单自动好评processFunction
     * 经过 interval 时间用户未给出评价的订单,自动给出好评,实际中应该是多少天之后，这里为了测试方便, interval 设置为5秒
     * 输入：<用户id,订单id,订单生成时间>
     * 输出：<用户id,订单id,订单生成时间，5>，即5星好评
     */
    public static class AutoFavorableKeyedProcessFunction
            extends KeyedProcessFunction<String, Tuple3<String, String, Long>, OrderComment> {

        // 用一个Map状态来存储订单id和订单信息
        private MapState<String, OrderComment> orderCommentState;

        // 用一个Map状态来存储订单id和定时器时间戳
        // private MapState<String, Long> orderIdTimerState;

        private final long interval;

        public AutoFavorableKeyedProcessFunction(long interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化两个状态信息
            MapStateDescriptor<String, OrderComment> descriptor =
                    new MapStateDescriptor<>("order-comment-state", String.class, OrderComment.class);
            this.orderCommentState = getRuntimeContext().getMapState(descriptor);

            //MapStateDescriptor<String, Long> orderIdTimerDescriptor =
            //        new MapStateDescriptor<>("orderId-Timer-State", String.class, Long.class);
            //this.orderIdTimerState = getRuntimeContext().getMapState(orderIdTimerDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, Long> t3, Context ctx, Collector<OrderComment> out) throws Exception {
            // 获取当前的时间
            long now = ctx.timerService().currentProcessingTime();
            // 订单创建时间
            Long orderCreateTime = t3.f2;

            // 如果订单创建时间比当前时间还大,表示为非法无效订单,直接抛弃
            if (orderCreateTime > now) {
                return;
            }
            // 如果当前时间和订单创建时间的差值大于 interval,则自动给予好评
            if (now - orderCreateTime > this.interval) {
                out.collect(new OrderComment(t3.f0, t3.f1, t3.f2, "5", now));
            } else {
                // 先把当前的订单信息存储到状态中
                this.orderCommentState.put(t3.f1, new OrderComment(t3.f0, t3.f1, t3.f2, null, null));
                // 注册一个定时器，在 (orderCreateTime + interval) 之后触发执行
                long timer = orderCreateTime + this.interval;
                // this.orderIdTimerState.put(t3.f1, timer); // 在状态中存储下商品id对应的定时器时间戳
                ctx.timerService().registerProcessingTimeTimer(timer); // 注册定时器
            }
        }

        /**
         * 定时器触发逻辑
         *
         * @param timestamp 定时器的时间戳
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderComment> out) throws Exception {
            // 拿到订单信息,并添加好评
            for (Iterator<OrderComment> iterator = this.orderCommentState.values().iterator(); iterator.hasNext(); ) {
                OrderComment orderComment = iterator.next();
                // 找到时间到期且并未获得评价的,系统给予五星好评；实际应用中需要去吊用订单评价系统，看用户有没有已经添加过评价，这边用一个方法模拟一下
                if ((orderComment.getCreateTime() + this.interval) == timestamp && hasNotCommented(orderComment)) {
                    // 系统自动给予5星好评
                    orderComment.setComment("5");
                    // 填充自动好评处理时间
                    orderComment.setAutoFavorableTime(timestamp);
                    // 写出去
                    out.collect(orderComment);

                    // 任务完成后,清理定时器
                    ctx.timerService().deleteProcessingTimeTimer(timestamp);
                    // 同时为了避免重复评价,也要移出已经评价过的订单信息
                    iterator.remove(); // 从系统中移除
                    this.orderCommentState.remove(orderComment.getOrderId()); // 从状态中移除
                }
            }
        }

        /**
         * 模拟访问订单评价系统，验证该商品是否已经被评价过
         *
         * @param orderComment
         * @return
         */
        public boolean hasNotCommented(OrderComment orderComment) {
            // 1. 去访问订单评价系统,判断该订单是否被评价过
            // ...

            // 返回是否已经被评价
            return StringUtils.isEmpty(orderComment.getComment());
        }
    }

    /**
     * 自定义sourceFunction
     * 产出的数据<用户id,订单id,订单生成时间>
     */
    public static class MySourceFunction extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private boolean flag = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            while (flag) {
                String orderId = "order-" + RandomUtil.randomString(5);
                String userId = "user-" + RandomUtil.randomString(5);
                ctx.collect(Tuple3.of(orderId, userId, System.currentTimeMillis()));
                Thread.sleep(500L);
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }

    // 订单评价类
    public static class OrderComment {
        private String orderId;
        private String userId;
        private Long createTime;
        // 好评时间
        private String comment;
        private Long autoFavorableTime;

        public OrderComment() {
        }

        public OrderComment(String orderId, String userId, Long createTime, String comment, Long autoFavorableTime) {
            this.orderId = orderId;
            this.userId = userId;
            this.createTime = createTime;
            this.comment = comment;
            this.autoFavorableTime = autoFavorableTime;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(Long createTime) {
            this.createTime = createTime;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public Long getAutoFavorableTime() {
            return autoFavorableTime;
        }

        public void setAutoFavorableTime(Long autoFavorableTime) {
            this.autoFavorableTime = autoFavorableTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderComment that = (OrderComment) o;
            return Objects.equals(orderId, that.orderId) && Objects.equals(userId, that.userId) && Objects.equals(createTime, that.createTime) && Objects.equals(comment, that.comment) && Objects.equals(autoFavorableTime, that.autoFavorableTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, userId, createTime, comment, autoFavorableTime);
        }

        @Override
        public String toString() {
            return "OrderComment{" +
                    "orderId='" + orderId + '\'' +
                    ", userId='" + userId + '\'' +
                    ", createTime=" + createTime +
                    ", comment='" + comment + '\'' +
                    ", autoFavorableTime=" + autoFavorableTime +
                    '}';
        }
    }
}
