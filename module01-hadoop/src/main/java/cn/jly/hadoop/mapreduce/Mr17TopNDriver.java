package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @author lanyangji
 * @date 2021/4/27 下午 7:40
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr17TopNDriver
 */
public class Mr17TopNDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();
        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr17TopNDriver.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\topn\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\topn\\output1"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class TopNMapper extends Mapper<LongWritable, Text, Flow, Text> {
        // treeMap天然按照key排序
        private TreeMap<Flow, Text> topFlowMap = new TreeMap<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final Flow k = new Flow();
            final Text v = new Text();
            
            final String[] fields = value.toString().split("\t");
            String phone = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);
            long sumFlow = Long.parseLong(fields[3]);

            k.setUpFlow(upFlow);
            k.setDownFlow(downFlow);
            k.setSumFlow(sumFlow);
            v.set(phone);

            topFlowMap.put(k, v);
            // 只写10个，那么reduce端收到的数据最多为 mapTask.num * 10，最少 1 * 10
            // 超过10个就把最小的流量删除
            if (topFlowMap.size() > 10) {
                topFlowMap.remove(topFlowMap.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Flow, Text> entry : topFlowMap.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class TopNReducer extends Reducer<Flow, Text, Text, Flow> {
        private TreeMap<Flow, Text> flowMap = new TreeMap<>();

        @Override
        protected void reduce(Flow key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                final Flow flow = new Flow();
                flow.setUpFlow(key.upFlow);
                flow.setDownFlow(key.downFlow);
                flow.setSumFlow(key.sumFlow);
                flowMap.put(flow, value);

                if (flowMap.size() > 10) {
                    flowMap.remove(flowMap.lastKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Flow, Text> entry : flowMap.entrySet()) {
                context.write(entry.getValue(), entry.getKey());
            }
        }
    }

    public static class Flow implements WritableComparable<Flow> {
        private long upFlow;
        private long downFlow;
        private long sumFlow;

        @Override
        public int compareTo(Flow o) {
            return -Long.compare(this.sumFlow, o.sumFlow);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.upFlow);
            out.writeLong(this.downFlow);
            out.writeLong(this.sumFlow);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.upFlow = in.readLong();
            this.downFlow = in.readLong();
            this.sumFlow = in.readLong();
        }

        @Override
        public String toString() {
            return String.join("\t",
                    String.valueOf(this.upFlow),
                    String.valueOf(this.downFlow),
                    String.valueOf(this.sumFlow));
        }

        public long getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(long upFlow) {
            this.upFlow = upFlow;
        }

        public long getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(long downFlow) {
            this.downFlow = downFlow;
        }

        public long getSumFlow() {
            return sumFlow;
        }

        public void setSumFlow(long sumFlow) {
            this.sumFlow = sumFlow;
        }
    }
}
