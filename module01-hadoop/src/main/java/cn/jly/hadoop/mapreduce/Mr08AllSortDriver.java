package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Objects;

/**
 * 全排序
 *
 * @author lanyangji
 * @date 2021/4/22 下午 4:26
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr08AllSort
 */
public class Mr08AllSortDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr08AllSortDriver.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);
        job.setMapOutputKeyClass(FlowEntity.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FlowEntity.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\flowcount\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\flowcount\\allsout3"));
        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class FlowCountMapper extends Mapper<LongWritable, Text, FlowEntity, Text> {
        private FlowEntity k = new FlowEntity();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");
            final String phoneNumber = fields[1];
            final long upFlow = Long.parseLong(fields[fields.length - 3].trim());
            final long downFlow = Long.parseLong(fields[fields.length - 2].trim());

            v.set(phoneNumber);
            k.setUpFlow(upFlow);
            k.setDownFlow(downFlow);
            k.setSumFlow(k.getUpFlow() + k.getDownFlow());

            context.write(k, v);
        }
    }

    /**
     * reducer
     */
    public static final class FlowCountReducer extends Reducer<FlowEntity, Text, FlowEntity, Text> {
        @Override
        protected void reduce(FlowEntity key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 循环写出，避免总流量相同
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    /**
     * 自定义序列化bean
     */
    public static class FlowEntity implements WritableComparable<FlowEntity> {
        private Long upFlow;
        private Long downFlow;
        private Long sumFlow;

        public FlowEntity() {
        }

        /**
         * 按照总流量倒序排序
         *
         * @param o
         * @return
         */
        @Override
        public int compareTo(FlowEntity o) {
            return -Long.compare(this.sumFlow, o.sumFlow);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(upFlow);
            out.writeLong(downFlow);
            out.writeLong(sumFlow);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.upFlow = in.readLong();
            this.downFlow = in.readLong();
            this.sumFlow = in.readLong();
        }

        public Long getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(Long upFlow) {
            this.upFlow = upFlow;
        }

        public Long getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(Long downFlow) {
            this.downFlow = downFlow;
        }

        public Long getSumFlow() {
            return sumFlow;
        }

        public void setSumFlow(Long sumFlow) {
            this.sumFlow = sumFlow;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FlowEntity flowEntity = (FlowEntity) o;
            return Objects.equals(upFlow, flowEntity.upFlow) && Objects.equals(downFlow, flowEntity.downFlow) && Objects.equals(sumFlow, flowEntity.sumFlow);
        }

        @Override
        public int hashCode() {
            return Objects.hash(upFlow, downFlow, sumFlow);
        }

        @Override
        public String toString() {
           return String.join("\t", String.valueOf(this.upFlow), String.valueOf(this.downFlow), String.valueOf(this.sumFlow));
        }
    }
}
