package cn.jly.bigdata.flink_advanced.datastream.c10_broadcast_state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 在开发过程中，如果遇到需要下发/广播配置、规则等低吞吐事件流到下游所有 task 时，就可以使用 Broadcast State。
 * Broadcast State 是 Flink 1.5 引入的新特性。
 * 下游的 task 接收这些配置、规则并保存为 BroadcastState, 将这些配置应用到另一个数据流的计算中 。
 * <p>
 * 场景举例
 * 1) 动态更新计算规则: 如事件流需要根据最新的规则进行计算，则可将规则作为广播状态广播到下游Task中。
 * 2）实时增加额外字段: 如事件流需要实时增加用户的基础信息，则可将用户的基础信息作为广播状态广播到下游Task中。
 * <p>
 * 需求：
 * 实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
 * 1. 事件流：表示用户在某个时刻浏览或点击了某个商品，格式如下。
 * {"userID": "user_3", "eventTime": "2019-08-17 12:19:47", "eventType": "browse", "productID": 1}
 * {"userID": "user_2", "eventTime": "2019-08-17 12:19:48", "eventType": "click", "productID": 1}
 * 2. mysql中存储的用户信息，成为配置流、规则流或者用户信息流：
 * <id, name, age>
 * <p>
 * 要求输出：
 * <userId, productId, eventTime, eventType, name, age>
 *
 * @author jilanyang
 * @date 2021/8/8 15:29
 * @package cn.jly.bigdata.flink_advanced.datastream.c10_broadcast_state
 * @class D01_BroadcastState
 */
public class D01_BroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 用户事件流 - 数据量较大
        DataStreamSource<UserEvent> userEventDS = env.addSource(new UserEventSourceFunction());
        //userEventDS.print();

        // mysql中的配置流/规则流/用户信息流 <userId, <name, age>> - 数据量较小
        DataStreamSource<Map<String, Tuple2<String, Integer>>> userInfoMapDS = env.addSource(new MysqlUserInfoSourceFunction(
                "jdbc:mysql://linux01:3306/test",
                "root",
                "123456"
        ));
        //userInfoMapDS.printToErr();

        // 广播配置流
        // map状态描述符
        //MapStateDescriptor<String, Tuple2<String, Integer>> descriptor =
        //        new MapStateDescriptor<>("user-info_map", Types.STRING, Types.TUPLE(Types.STRING, Types.INT));
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor =
                new MapStateDescriptor<>("user-info-map", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastStream = userInfoMapDS.broadcast(descriptor);

        // 早于广播配置流的事件流可能会无法被处理,这边以侧输出流单独采集输出,最后统一处理
        OutputTag<UserEvent> earlyEventTag = new OutputTag<UserEvent>("early-event-output-tage", Types.POJO(UserEvent.class)) {
        };

        // 两个流连接
        BroadcastConnectedStream<UserEvent, Map<String, Tuple2<String, Integer>>> connectDS = userEventDS.connect(broadcastStream);
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Integer>> resDS =
                connectDS.process(new AddUserInfoBroadcastProcessFunction(descriptor, earlyEventTag));

        // 输出
        resDS.print();

        // 早于广播配置流到达的事件流没有被处理,这里以侧输出流单独处理,可以选择写到某个介质中,最后单独处理)
        DataStream<UserEvent> earlyEventDS = resDS.getSideOutput(earlyEventTag);
        earlyEventDS.printToErr("earlyEventDS");

        env.execute("D01_BroadcastState");
    }

    /**
     * 自定义BroadcastProcessFunction
     * 需求：实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
     */
    public static class AddUserInfoBroadcastProcessFunction
            extends BroadcastProcessFunction<UserEvent, Map<String, Tuple2<String, Integer>>, Tuple6<String, String, String, String, String, Integer>> {

        private final MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor;

        private final OutputTag<UserEvent> earlyEventTag;

        public AddUserInfoBroadcastProcessFunction(MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor, OutputTag<UserEvent> earlyEventTag) {
            this.descriptor = descriptor;
            this.earlyEventTag = earlyEventTag;
        }

        /**
         * 处理事件流中的每一个元素
         *
         * @param userEvent
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(UserEvent userEvent, ReadOnlyContext ctx, Collector<Tuple6<String, String, String, String, String, Integer>> out) throws Exception {
            // 目标是为了将事件流中的数据和广播流的数据进行关联,进而输出<userId, productId, eventTime, eventType, name, age>
            // 1. 获取广播流 - 只读的
            ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(this.descriptor);
            // 拿到广播流中的数据
            Map<String, Tuple2<String, Integer>> userInfoMap = broadcastState.get(null);
            if (userInfoMap != null){
                // 2. 组装数据并输出--- 需求：实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
                if (userInfoMap.containsKey(userEvent.getUserId())) {
                    Tuple2<String, Integer> nameAndAge = userInfoMap.get(userEvent.getUserId());
                    // 输出
                    out.collect(Tuple6.of(userEvent.getUserId(), userEvent.getProductId(), userEvent.getEventTime(), userEvent.getEventType(), nameAndAge.f0, nameAndAge.f1));
                }
            } else {
                // 这边有一个问题,假设事件流的数据已经到来,但是下面processBroadcastElement方法更新状态的数据发生在之后,这就可能导致早于广播状态更新之前的事件流没有被处理(收集)
                // 推荐解决办法,用侧输出流先把数据收集到某一个存储介质中,最后来单独处理
                ctx.output(this.earlyEventTag, userEvent);
            }
        }

        /**
         * 更新处理广播流中的数据
         *
         * @param map
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Map<String, Tuple2<String, Integer>> map, Context ctx, Collector<Tuple6<String, String, String, String, String, Integer>> out) throws Exception {
            // map就是最新的广播数据(从mysql中每隔5s查询来的并广播到状态中的数据)
            // 所以这里要做的就是把数据广播到状态中
            // 1. 先获取状态中已有的数据
            BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(this.descriptor);
            // 2. 清空旧数据
            broadcastState.clear();
            // 3. 更新新数据
            broadcastState.put(null, map);
        }
    }

    /**
     * 从Mysql中定时查询用户信息的数据源
     * 每次输出一个全量用户map, key：userId, value:<name, age>
     */
    public static class MysqlUserInfoSourceFunction extends RichSourceFunction<Map<String, Tuple2<String, Integer>>> {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private ResultSet resultSet;
        private boolean flag = true;

        public MysqlUserInfoSourceFunction(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            preparedStatement = connection.prepareStatement("select id, name, age from tbl_user");
        }

        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
            while (flag) {
                // 每次查询把查询的所有结果封装为一个map
                HashMap<String, Tuple2<String, Integer>> userMap = new HashMap<>();
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    int userId = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int age = resultSet.getInt("age");

                    userMap.put(String.valueOf(userId), Tuple2.of(name, age));
                }
                ctx.collect(userMap);

                // 每5秒更新一次
                Thread.sleep(5000);
            }
        }

        @Override
        public void close() throws Exception {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }

    /**
     * 用户事件流
     */
    public static class UserEventSourceFunction extends RichParallelSourceFunction<UserEvent> {
        private boolean flag = true;
        private final Random random = new Random();
        private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        // 这边暂时以点击、浏览、下单、支付四个事件举例
        private final String[] eventTypes = new String[]{"click", "browse", "order", "pay"};

        @Override
        public void run(SourceContext<UserEvent> ctx) throws Exception {
            while (flag) {
                String userId = String.valueOf(random.nextInt(21));
                String productId = String.valueOf(random.nextInt(100));
                String eventTime = dateFormat.format(new Date());
                String eventType = eventTypes[random.nextInt(eventTypes.length)];
                ctx.collect(new UserEvent(userId, productId, eventTime, eventType));

                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }

    /**
     * 用户基本事件pojo
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserEvent {
        private String userId;
        private String productId;
        private String eventTime;
        private String eventType;
    }
}
