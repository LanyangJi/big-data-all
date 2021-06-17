package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/26 上午 11:36
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr14EtlDriver
 */
public class Mr14EtlDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr14EtlDriver.class);
        job.setMapperClass(EtlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\etl\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\etl\\output"));
        job.setNumReduceTasks(0);
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class LogBean {
        // 记录客户端的ip地址
        private String remoteAddr;
        // 记录客户端用户名称,忽略属性"-"
        private String remoteUser;
        // 记录访问时间与时区
        private String timeLocal;
        // 记录请求的url与http协议
        private String request;
        // 记录请求状态；成功是200
        private String status;
        // 记录发送给客户端文件主体内容大小
        private String bodyBytesSent;
        // 用来记录从那个页面链接访问过来的
        private String httpReferer;
        // 记录客户浏览器的相关信息
        private String httpUserAgent;
        // 判断数据是否合法
        private boolean valid = true;

        @Override
        public String toString() {
            return String.join("\t",
                    String.valueOf(this.valid),
                    this.remoteAddr,
                    this.remoteUser,
                    this.request,
                    this.status,
                    this.bodyBytesSent,
                    this.httpReferer,
                    this.httpUserAgent);
        }

        public String getRemoteAddr() {
            return remoteAddr;
        }

        public void setRemoteAddr(String remoteAddr) {
            this.remoteAddr = remoteAddr;
        }

        public String getRemoteUser() {
            return remoteUser;
        }

        public void setRemoteUser(String remoteUser) {
            this.remoteUser = remoteUser;
        }

        public String getTimeLocal() {
            return timeLocal;
        }

        public void setTimeLocal(String timeLocal) {
            this.timeLocal = timeLocal;
        }

        public String getRequest() {
            return request;
        }

        public void setRequest(String request) {
            this.request = request;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getBodyBytesSent() {
            return bodyBytesSent;
        }

        public void setBodyBytesSent(String bodyBytesSent) {
            this.bodyBytesSent = bodyBytesSent;
        }

        public String getHttpReferer() {
            return httpReferer;
        }

        public void setHttpReferer(String httpReferer) {
            this.httpReferer = httpReferer;
        }

        public String getHttpUserAgent() {
            return httpUserAgent;
        }

        public void setHttpUserAgent(String httpUserAgent) {
            this.httpUserAgent = httpUserAgent;
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }
    }

    /**
     * mapper
     */
    public static class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 过滤
            final LogBean logBean = parseLog(value, context);

            if (!logBean.isValid()) {
                return;
            }

            k.set(logBean.toString());
            context.write(k, NullWritable.get());
        }

        private LogBean parseLog(Text value, Context context) {
            final LogBean logBean = new LogBean();

            final String[] fields = value.toString().split(" ");
            if (fields.length > 11) {
                logBean.setRemoteAddr(fields[0]);
                logBean.setRemoteUser(fields[1]);
                logBean.setTimeLocal(fields[3].substring(1));
                logBean.setRequest(fields[6]);
                logBean.setStatus(fields[8]);
                logBean.setBodyBytesSent(fields[9]);
                logBean.setHttpReferer(fields[10]);

                if (fields.length > 12) {
                    logBean.setHttpUserAgent(fields[11] + " " + fields[12]);
                } else {
                    logBean.setHttpUserAgent(fields[11]);
                }

                // 大于400，http错误
                if (Integer.parseInt(logBean.getStatus()) >= 400) {
                    logBean.setValid(false);
                    context.getCounter("lanyangji", "invalid-jly-count").increment(1);
                }

                // 计数器
                context.getCounter("lanyangji", "valid-jly-count").increment(1);
            } else {
                logBean.setValid(false);
                context.getCounter("lanyangji", "invalid-jly-count").increment(1);
            }

            return logBean;
        }
    }
}
