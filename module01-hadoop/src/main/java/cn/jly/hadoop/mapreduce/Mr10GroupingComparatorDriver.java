package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
 * 求出每一个订单中最贵的商品
 * <p>
 * （1）利用“订单id和成交金额”作为key，可以将Map阶段读取到的所有订单数据按照id升序排序，如果id相同再按照金额降序排序，发送到Reduce。
 * （2）在Reduce端利用groupingComparator将订单id相同的kv聚合成组，然后取第一个即是该订单中最贵商品，
 *
 * @author lanyangji
 * @date 2021/4/22 下午 9:00
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr10GroupingComparatorDriver
 */
public class Mr10GroupingComparatorDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr09CombinerDriver.class);
        job.setMapperClass(GroupingComparatorMapper.class);
        job.setReducerClass(GroupingComparatorReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\groupingcomparator\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\groupingcomparator\\output"));

        // 设置分组排序类
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class GroupingComparatorMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        private final OrderBean k = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");
            final int orderId = Integer.parseInt(fields[0]);
            final double price = Double.parseDouble(fields[2]);
            k.setOrderId(orderId);
            k.setPrice(price);

            context.write(k, NullWritable.get());
        }
    }

    /**
     * 分组排序
     * orderId相同则分到同一组
     */
    public static class OrderSortGroupingComparator extends WritableComparator {
        public OrderSortGroupingComparator() {
            super(OrderBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            OrderBean aBean = (OrderBean) a;
            OrderBean bBean = (OrderBean) b;

            // orderId相同则分到同一组
            return -Integer.compare(aBean.orderId, bBean.orderId);
        }
    }

    /**
     * reducer
     */
    public static class GroupingComparatorReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // 直接写出第一个
            context.write(key, values.iterator().next());
        }
    }

    /**
     * bean
     */
    public static class OrderBean implements WritableComparable<OrderBean> {
        private int orderId;
        private double price;

        public OrderBean() {
        }

        public OrderBean(int orderId, double price) {
            this.orderId = orderId;
            this.price = price;
        }

        /**
         * 按照orderId排序，如果orderId相同，则按照price降序
         *
         * @param o
         * @return
         */
        @Override
        public int compareTo(OrderBean o) {
            if (this.orderId == o.orderId) {
                return -Double.compare(this.price, o.price);
            }
            return Integer.compare(this.orderId, o.orderId);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(orderId);
            out.writeDouble(price);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.orderId = in.readInt();
            this.price = in.readDouble();
        }

        public int getOrderId() {
            return orderId;
        }

        public void setOrderId(int orderId) {
            this.orderId = orderId;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OrderBean orderBean = (OrderBean) o;
            return orderId == orderBean.orderId && Double.compare(orderBean.price, price) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, price);
        }

        @Override
        public String toString() {
            return String.join("\t", String.valueOf(this.orderId), String.valueOf(this.price));
        }
    }
}
