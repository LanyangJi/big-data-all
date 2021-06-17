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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * @author lanyangji
 * @date 2021/4/23 上午 10:14
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr12ReduceJoinDriver
 */
public class Mr12ReduceJoinDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr12ReduceJoinDriver.class);
        job.setMapperClass(FileTableMapper.class);
        job.setReducerClass(FileTableReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderProductBean.class);
        job.setOutputKeyClass(OrderProductBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\reducejoin\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\reducejoin\\rj_output"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class FileTableMapper extends Mapper<LongWritable, Text, Text, OrderProductBean> {
        private String fileName;
        private Text k = new Text();
        private OrderProductBean v = new OrderProductBean();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 得到切片
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // 文件名
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");
            // 处理订单表
            if (fileName.startsWith("order")) {
                final long orderId = Long.parseLong(fields[0].trim());
                final long productId = Long.parseLong(fields[1].trim());
                final int amount = Integer.parseInt(fields[2].trim());

                k.set(fields[1]);
                v.setId(orderId);
                v.setPid(productId);
                v.setAmount(amount);
                v.setFlag("order");
                v.setProductName("");
            } else { // 处理商品表
                k.set(fields[0]);
                v.setPid(Long.parseLong(fields[0].trim()));
                v.setProductName(fields[1].trim());
                v.setFlag("product");
                v.setId(0L);
                v.setAmount(0);
            }

            context.write(k, v);
        }
    }

    /**
     * reducer
     * <p>
     * reduce join，以两表连接字段作为key进行聚合
     */
    public static class FileTableReducer extends Reducer<Text, OrderProductBean, OrderProductBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<OrderProductBean> values, Context context) throws IOException, InterruptedException {
            // 订单表集合
            final ArrayList<OrderProductBean> orderBeanList = new ArrayList<>();
            // 产品
            final OrderProductBean productBean = new OrderProductBean();

            for (OrderProductBean bean : values) {
                // 订单
                final OrderProductBean copyBean = new OrderProductBean();
                if ("order".equals(bean.getFlag())) {
                    try {
                        BeanUtils.copyProperties(copyBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    orderBeanList.add(copyBean);
                } else {
                    try {
                        BeanUtils.copyProperties(productBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // 组装目标数据
            for (OrderProductBean orderBean : orderBeanList) {
                orderBean.setProductName(productBean.getProductName());
                // 写出数据
                context.write(orderBean, NullWritable.get());
            }
        }
    }

    public static class OrderProductBean implements WritableComparable<OrderProductBean> {
        private Long id;
        private Long pid;
        private Integer amount;
        private String productName;
        // 文件标识
        private String flag;

        @Override
        public int compareTo(OrderProductBean o) {
            if (this.id.equals(o.id)) {
                return this.pid.compareTo(o.pid);
            }
            return this.id.compareTo(o.id);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(pid);
            out.writeInt(amount);
            out.writeUTF(productName);
            out.writeUTF(flag);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readLong();
            this.pid = in.readLong();
            this.amount = in.readInt();
            this.productName = in.readUTF();
            this.flag = in.readUTF();
        }

        @Override
        public String toString() {
            return String.join("\t", String.valueOf(this.id), this.productName, String.valueOf(this.amount));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OrderProductBean that = (OrderProductBean) o;
            return Objects.equals(id, that.id) && Objects.equals(pid, that.pid) && Objects.equals(amount, that.amount) && Objects.equals(productName, that.productName) && Objects.equals(flag, that.flag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, pid, amount, productName, flag);
        }

        public String getFlag() {
            return flag;
        }

        public void setFlag(String flag) {
            this.flag = flag;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getPid() {
            return pid;
        }

        public void setPid(Long pid) {
            this.pid = pid;
        }

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }

        public String getProductName() {
            return productName;
        }

        public void setProductName(String productName) {
            this.productName = productName;
        }
    }
}
