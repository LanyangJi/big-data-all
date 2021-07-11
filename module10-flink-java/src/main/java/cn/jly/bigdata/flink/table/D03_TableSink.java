package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lanyangji
 * @date 2021/7/9 14:30
 * @packageName cn.jly.bigdata.flink.table
 * @className D03_TableSink
 */
public class D03_TableSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.execute("D03_TableSink");
    }
}
