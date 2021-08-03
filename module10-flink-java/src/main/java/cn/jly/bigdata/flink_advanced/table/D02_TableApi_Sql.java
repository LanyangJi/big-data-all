package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * table api和sql两种方式做wordCount
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D02_TableApi_Sql
 * @date 2021/8/2 21:27
 */
public class D02_TableApi_Sql {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Wc> wordDs = env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Wc>() {
                    @Override
                    public void flatMap(String value, Collector<Wc> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Wc(word, 1));
                        }
                    }
                });

        // 方式一：sql方式统计，创建视图
        tableEnv.createTemporaryView("tbl_word", wordDs, $("word"), $("count"));
        String sql = "select word, sum(`count`) as sum_count from tbl_word group by word";
        Table resTable = tableEnv.sqlQuery(sql).as("word", "count");
        /*
            toAppendStream - 将计算后的结果append到结果DataStream中
            toRetractStream - 将计算后的新的数据在DataStream原数据的基础上更新true和删除false
         */
        DataStream<Tuple2<Boolean, Wc>> countDs = tableEnv.toRetractStream(resTable, Wc.class);
        countDs.print();

        // 方式二：table api
        Table table = tableEnv.fromDataStream(wordDs).as("word", "count");
        Table queryTable = table.groupBy($("word"))
                .select($("word"), $("count").sum().as("sum_count"))
                .filter($("sum_count").isGreater(2))
                .as("word", "count");
        DataStream<Tuple2<Boolean, Wc>> queryDs = tableEnv.toRetractStream(queryTable, Wc.class);
        queryDs.printToErr();

        env.execute("D02_TableApi_Sql");
    }

    public static class Wc {
        private String word;
        private Integer count;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Wc wc = (Wc) o;
            return Objects.equals(word, wc.word) && Objects.equals(count, wc.count);
        }

        @Override
        public int hashCode() {
            return Objects.hash(word, count);
        }

        @Override
        public String toString() {
            return "Wc{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public Wc(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public Wc() {
        }
    }
}
