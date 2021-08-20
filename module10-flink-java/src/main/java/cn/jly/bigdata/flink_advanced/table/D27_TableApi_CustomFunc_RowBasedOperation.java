package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import jdk.internal.org.objectweb.asm.TypeReference;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义函数，基于行的操作
 *
 * todo 程序有问题，等到学到用户自定义函数位置完善当前例子
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/tableapi/#row-based-operations
 *
 * @author jilanyang
 * @createTime 2021/8/20 15:49
 */
public class D27_TableApi_CustomFunc_RowBasedOperation {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source - 订单流
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("localhost", 9999)
                .flatMap(
                        new FlatMapFunction<String, Order>() {
                            @Override
                            public void flatMap(String s, Collector<Order> collector) throws Exception {
                                String[] fields = s.split(",");
                                String orderId = fields[0];
                                String userId = fields[1];
                                long createTime = Long.parseLong(fields[2]);
                                double money = Double.parseDouble(fields[3]);

                                collector.collect(new Order(orderId, userId, createTime, money));
                            }
                        }
                );

        Table orderTable = tableEnv.fromDataStream(orderDS);

        // 注册自定义函数
        MyMapFunction mapFunction = new MyMapFunction();
        tableEnv.createTemporaryFunction("func", mapFunction);

        Table resTable = orderTable.map(call("func", $("userId")));

        // 输出
        tableEnv.toAppendStream(resTable, Row.class).print();

        env.execute("D27_TableApi_CustomFunc_RowBasedOperation");
    }

    // 使用用户定义的标量函数或内置标量函数执行映射操作。如果输出类型是复合类型，则输出将被展平。
    public static class MyMapFunction extends ScalarFunction {
        public Row eval(String input) {
            return Row.of(input, "prefix-" + input);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .typedArguments(
                            typeFactory.createDataType(String.class),
                            typeFactory.createDataType(String.class)
                    )
                    .outputTypeStrategy(TypeStrategies.ROW)
                    .build();
        }
    }
}
