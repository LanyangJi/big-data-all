package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * table api和 sql的标准写法
 *
 * @author jilanyang
 * @date 2021/7/15 21:48
 */
public class D03_Standard {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建输入表

        // 创建输出表

    }
}
