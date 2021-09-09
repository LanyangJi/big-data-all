package cn.jly.bigdata.flink_advanced.datastream.c16_distributed_cache_file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * flink 分布式缓存测试
 * <p>
 * Flink提供了一个类似于Hadoop的分布式缓存，让并行运行实例的函数可以在本地访问。
 * 这个功能可以被使用来分享外部静态的数据，例如：机器学习的逻辑回归模型等
 * <p>
 * 注意：
 * 广播变量是将变量分发到各个TaskManager节点的内存上，分布式缓存是将文件缓存到各个 TaskManager节点上；
 *
 * @author jilanyang
 * @date 2021/9/9 19:25
 */
public class D01_DistributedCacheFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 注册分布式缓存文件 <部门id，部门名称>
        env.registerCachedFile("file:///D:/workspace/jly/big-data-all/input/dept.csv", "dept");

        // 员工数据流
        env.socketTextStream("localhost", 9999)
                .flatMap(
                        new RichFlatMapFunction<String, Tuple3<String, String, String>>() {
                            private final Map<String, Department> map = new HashMap<>();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 获取分布式缓存文件
                                File deptFile = getRuntimeContext().getDistributedCache().getFile("dept");
                                List<String> contents = FileUtils.readLines(deptFile, "utf-8");
                                for (String content : contents) {
                                    String[] fields = content.split(",");
                                    map.put(fields[0], new Department(fields[0], fields[1]));
                                }
                            }

                            @Override
                            public void flatMap(String s, Collector<Tuple3<String, String, String>> collector) throws Exception {
                                String[] fields = s.split(" ");
                                Employ employ = new Employ(fields[0], fields[1], fields[2]);
                                // 拿到部门信息
                                Department department = this.map.getOrDefault(employ.getDeptId(), new Department("other", "other"));

                                collector.collect(Tuple3.of(employ.getId(), employ.getName(), department.getDeptName()));
                            }
                        }
                )
                .print();

        env.execute("D01_DistributedCacheFile");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Employ {
        private String id;
        private String name;
        private String deptId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Department {
        private String id;
        private String deptName;
    }
}
