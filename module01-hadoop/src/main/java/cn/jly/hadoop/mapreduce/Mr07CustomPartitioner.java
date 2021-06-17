package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分区器
 *
 * @author lanyangji
 * @date 2021/4/20 下午 9:35
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr02BeanSerializable
 */
public class Mr07CustomPartitioner extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);

        job.setJarByClass(Mr07CustomPartitioner.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\flowcount\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\flowcount\\output4"));

        // 指定自定义分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        // 指定相应数量的reduceTasks
        job.setNumReduceTasks(5);

        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    /**
     * 自定义分区器
     */
    public static class ProvincePartitioner extends Partitioner<Text, FlowBean> {

        @Override
        public int getPartition(Text text, FlowBean flowBean, int i) {
            final String preNum = text.toString().substring(0, 3);
            int partition = 4;

            switch (preNum) {
                case "136":
                    partition = 0;
                    break;
                case "137":
                    partition = 1;
                    break;
                case "138":
                    partition = 2;
                    break;
                case "139":
                    partition = 3;
                    break;
                default:
                    break;
            }

            return partition;
        }
    }

    /**
     * mapper
     * <p>
     * 输入
     * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
     * 2	13846544121	192.196.100.2			264	0	200
     * 3 	13956435636	192.196.100.3			132	1512	200
     * 4 	13966251146	192.168.100.1			240	0	404
     * 5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
     * 6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
     * 7 	13590439668	192.168.100.4			1116	954	200
     * 8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
     * 9 	13729199489	192.168.100.6			240	0	200
     * 10 	13630577991	192.168.100.7	www.shouhu.com	6960	690	200
     * 11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
     * 12 	15959002129	192.168.100.9	www.atguigu.com	1938	180	500
     * 13 	13560439638	192.168.100.10			918	4938	200
     * 14 	13470253144	192.168.100.11			180	180	200
     * 15 	13682846555	192.168.100.12	www.qq.com	1938	2910	200
     * 16 	13992314666	192.168.100.13	www.gaga.com	3008	3720	200
     * 17 	13509468723	192.168.100.14	www.qinghua.com	7335	110349	404
     * 18 	18390173782	192.168.100.15	www.sogou.com	9531	2412	200
     * 19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
     * 20 	13768778790	192.168.100.17			120	120	200
     * 21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
     * 22 	13568436656	192.168.100.19			1116	954	200
     */
    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        private final Text k = new Text();
        private final FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");

            k.set(fields[1]);
            v.setUpFlow(Long.parseLong(fields[fields.length - 3].trim()));
            v.setDownFlow(Long.parseLong(fields[fields.length - 2].toString()));
            v.setSumFlow(v.getUpFlow() + v.getDownFlow());

            context.write(k, v);
        }
    }

    /**
     * reducer
     */
    public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        private final FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sumUpFlow = 0;
            long sumDownFlow = 0;
            long totalFlow = 0;
            for (FlowBean value : values) {
                sumUpFlow += value.getUpFlow();
                sumDownFlow += value.getDownFlow();
                totalFlow += value.getSumFlow();
            }

            v.setUpFlow(sumUpFlow);
            v.setDownFlow(sumDownFlow);
            v.setSumFlow(totalFlow);

            context.write(key, v);
        }
    }

    public static class FlowBean implements WritableComparable<FlowBean> {
        private long upFlow;
        private long downFlow;
        private long sumFlow;

        public FlowBean() {
        }

        /**
         * 序列化方法
         *
         * @param out
         * @throws IOException
         */
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.upFlow);
            out.writeLong(this.downFlow);
            out.writeLong(this.sumFlow);
        }

        /**
         * 反序列化方法
         *
         * @param in
         * @throws IOException
         */
        @Override
        public void readFields(DataInput in) throws IOException {
            this.upFlow = in.readLong();
            this.downFlow = in.readLong();
            this.sumFlow = in.readLong();
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

        @Override
        public String toString() {
            return String.join("\t", String.valueOf(upFlow), String.valueOf(downFlow), String.valueOf(sumFlow));
        }

        /**
         * 按照总流量倒序排序
         * <p>
         * hadoop是针对key进行排序
         *
         * @param o
         * @return
         */
        @Override
        public int compareTo(FlowBean o) {
            return -Long.compare(this.sumFlow, o.sumFlow);
        }
    }
}
