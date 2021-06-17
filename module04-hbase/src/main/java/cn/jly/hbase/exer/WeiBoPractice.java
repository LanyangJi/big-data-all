package cn.jly.hbase.exer;

import cn.hutool.core.collection.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.jamon.emit.EmitMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 微博实战
 * 1) 微博内容的浏览，数据库表设计
 * 2) 用户社交体现：关注用户，取关用户
 * 3) 拉取关注的人的微博内容
 * <p>
 * 设计
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 * 9) 测试
 *
 * @author Administrator
 * @date 2021/5/25 0025 16:08
 * @packageName cn.jly.hbase.cn.spark.core.exer
 * @className WeiBoPractice
 */
public class WeiBoPractice {
    // 配置文件
    private Configuration conf = HBaseConfiguration.create();

    // 微博内容表名
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("weibo:context");
    // 用户关系表名
    private static final byte[] TABLE_RELATION = Bytes.toBytes("weibo:relation");
    // 收件箱
    private static final byte[] TABLE_RECEIVE_EMAIL = Bytes.toBytes("weibo:receive_email");


    /**
     * 创建命令空间
     *
     * @param namespace
     * @param confMap
     * @throws Exception
     */
    public void initNamespace(String namespace, Map<String, String> confMap) throws Exception {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin()
        ) {
            final NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace)
                    .addConfiguration(confMap)
                    .build();
            admin.createNamespace(descriptor);
        }
    }

    /**
     * 创建微博内容表
     * 列族：info
     * 列：标题，内容，图片
     *
     * @throws IOException
     */
    public void createTableContent() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            final TableName tableName = TableName.valueOf(TABLE_CONTENT);
            // 表
            final TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
                    (TableDescriptorBuilder.ModifyableTableDescriptor) TableDescriptorBuilder.newBuilder(tableName).build();

            // 列族
            final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnFamilyDescriptor =
                    (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes(StandardCharsets.UTF_8)).build();
            // 设置块缓存
            columnFamilyDescriptor.setBlockCacheEnabled(true);
            // 设置块缓存大小
            columnFamilyDescriptor.setBlocksize(2097152);
            // 设置压缩类型
            columnFamilyDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
            // 设置版本确界
            columnFamilyDescriptor.setMaxVersions(1);
            columnFamilyDescriptor.setMinVersions(1);

            tableDescriptor.setColumnFamily(columnFamilyDescriptor);
            admin.createTable(tableDescriptor);
        }
    }

    /**
     * 创建用户关系表
     * 列族：attends, followers
     * 列：关注者id，粉丝id
     * 值：用户id
     *
     * @throws IOException
     */
    public void createTableRelation() throws IOException {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin()
        ) {
            final TableName tableName = TableName.valueOf(TABLE_RELATION);
            final TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
                    (TableDescriptorBuilder.ModifyableTableDescriptor) TableDescriptorBuilder.newBuilder(tableName).build();
            // 关注的人列族
            final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor attends =
                    (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("attends")).build();
            // 设置块缓存
            attends.setBlockCacheEnabled(true);
            // 设置块缓存大小
            attends.setBlocksize(2 * 1024 * 1024);
            // 设置压缩方式
            attends.setCompressionType(Compression.Algorithm.SNAPPY);
            // 设置版本确界
            attends.setMaxVersions(1);
            attends.setMinVersions(1);


            // 粉丝列族
            final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor followers =
                    (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("followers")).build();
            followers.setBlockCacheEnabled(true);
            followers.setBlocksize(2 * 1024 * 1024);
            followers.setCompressionType(Compression.Algorithm.SNAPPY);
            followers.setMaxVersions(1);
            followers.setMinVersions(1);

            tableDescriptor.setColumnFamily(attends);
            tableDescriptor.setColumnFamily(followers);
            admin.createTable(tableDescriptor);
        }
    }

    /**
     * 创建收件箱表
     * 列族：info
     * 列： 用户id
     * 值: 微博内容的rowKey
     * 版本1000
     */
    public void createTableReceiveEmail() throws IOException {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin()
        ) {
            final TableName tableName = TableName.valueOf(TABLE_RECEIVE_EMAIL);
            // 表
            final TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
                    (TableDescriptorBuilder.ModifyableTableDescriptor) TableDescriptorBuilder.newBuilder(tableName).build();

            // 列族
            final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor =
                    (ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build();
            familyDescriptor.setBlockCacheEnabled(true);
            familyDescriptor.setBlocksize(2 * 1024 * 1024);
            familyDescriptor.setMaxVersions(1000);
            familyDescriptor.setMinVersions(1000);

            tableDescriptor.setColumnFamily(familyDescriptor);
            admin.createTable(tableDescriptor);
        }
    }

    /**
     * 发布内容，内容表加一条记录，收件箱也得到一条记录
     *
     * @param userId
     * @param content
     * @throws IOException
     */
    public void publishContent(String userId, String content) throws IOException {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                final Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
                final Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
                final Table emailTable = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL))
        ) {
            // 内容表插入记录
            long timestamp = System.currentTimeMillis();
            String rowKey = userId + "_" + timestamp;
            final Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), timestamp, Bytes.toBytes(content));
            contentTable.put(put);

            // 向用户的粉丝的收件箱插入数据
            // 1. 先从用户关系表中找出当前用户的所有粉丝
            Get get = new Get(Bytes.toBytes(userId));
            get.addFamily(Bytes.toBytes("followers"));
            final Result result = relationTable.get(get);
            List<byte[]> followers = new ArrayList<>();
            // 列名就是粉丝的用户id
            for (Cell cell : result.rawCells()) {
                followers.add(CellUtil.cloneQualifier(cell));
            }
            // 没有粉丝，直接退出
            if (followers.isEmpty()) {
                return;
            }
            // 操作收件箱表
            List<Put> puts = new ArrayList<>();
            for (byte[] follower : followers) {
                final Put emailPut = new Put(follower);
                emailPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(userId), timestamp, Bytes.toBytes(rowKey));
                puts.add(put);
            }
            emailTable.put(puts);
        }
    }

    /**
     * 添加关注用户
     * a、在微博用户关系表中，对当前主动操作的用户添加新关注的好友
     * b、在微博用户关系表中，对被关注的用户添加新的粉丝
     * c、微博收件箱表中添加所关注的用户发布的微博
     *
     * @param userId
     * @param attends
     */
    public void addAttend(String userId, String... attends) throws IOException {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                final Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
                final Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
                final Table emailTable = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL))
        ) {
            List<Put> puts = new ArrayList<>();
            final Put attendPut = new Put(Bytes.toBytes(userId));
            for (String attend : attends) {
                // 给当前用户的关注者列族中添加关注者
                attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));

                // 给被关注者的粉丝列族中添加当前用户
                final Put followerPut = new Put(Bytes.toBytes(attend));
                followerPut.addColumn(Bytes.toBytes("followers"), Bytes.toBytes(userId), Bytes.toBytes(userId));
                puts.add(followerPut);
            }
            puts.add(attendPut);
            relationTable.put(puts);

            //微博收件箱表中添加所关注的用户发布的微博
            // 先从微博表中取出关注者发的微博
            List<byte[]> rowKeys = new ArrayList<>();
            final Scan scan = new Scan();
            for (String attend : attends) {
                RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(attend + "_"));
                scan.setFilter(rowFilter);
                final ResultScanner scanner = contentTable.getScanner(scan);
                for (Result result : scanner) {
                    final byte[] row = result.getRow();
                    rowKeys.add(row);
                }
            }
            // 将取出的微博 rowKey 放置于当前操作用户的收件箱中
            if (rowKeys.isEmpty()) {
                return;
            }

            List<Put> emailPuts = new ArrayList<>();
            for (byte[] rk : rowKeys) {
                final Put put = new Put(Bytes.toBytes(userId));
                final String rkStr = Bytes.toString(rk);
                final String[] uIdAndTimestamp = rkStr.split("_");
                final String uId = uIdAndTimestamp[0];
                final String timestamp = uIdAndTimestamp[1];

                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uId), Long.parseLong(timestamp), rk);
                emailPuts.add(put);
            }
            emailTable.put(emailPuts);
        }
    }

    /**
     * 移除（取关）用户
     * a、在微博用户关系表中，对当前主动操作的用户移除取关的好友(attends)
     * b、在微博用户关系表中，对被取关的用户移除粉丝
     * c、微博收件箱中删除取关的用户发布的微博
     *
     * @param userId
     * @param attends
     */
    public void removeAttends(String userId, String... attends) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(conf);
                final Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
                final Table mailTable = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL))
        ) {
            // 在微博用户关系表中，对被取关的用户移除粉丝
            List<Delete> fanDeletes = new ArrayList<>();
            // 在微博用户关系表中，对当前主动操作的用户移除取关的好友(attends)
            final Delete attendDelete = new Delete(Bytes.toBytes(userId));
            for (String attend : attends) {
                attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));

                final Delete delete = new Delete(Bytes.toBytes(attend));
                delete.addColumn(Bytes.toBytes("followers"), Bytes.toBytes(userId));
                fanDeletes.add(delete);
            }
            fanDeletes.add(attendDelete);
            relationTable.delete(fanDeletes);

            // 微博收件箱中删除取关的用户发布的微博
            final Delete mailDelete = new Delete(Bytes.toBytes(userId));
            Arrays.stream(attends).forEach(attend -> mailDelete.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend)));
            mailTable.delete(mailDelete);
        }
    }

    /**
     * 获取关注的人的微博内容
     * a、从微博收件箱中获取所关注的用户的微博 RowKey
     * b、根据获取的 RowKey，得到微博内容
     *
     * @param userId
     */
    public List<Message> getAttendContent(String userId) throws IOException {
        final ArrayList<Message> messages = new ArrayList<>();

        try (
                final Connection connection = ConnectionFactory.createConnection(conf);
                final Table mailTable = connection.getTable(TableName.valueOf(TABLE_RECEIVE_EMAIL));
                final Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT))
        ) {
            // 从微博收件箱中获取所关注的用户的微博 RowKey
            List<byte[]> rowKeys = new ArrayList<>();

            final Get get = new Get(Bytes.toBytes(userId));
            get.addFamily(Bytes.toBytes("info"));
            final Result result = mailTable.get(get);
            for (Cell cell : result.rawCells()) {
                rowKeys.add(CellUtil.cloneValue(cell));
            }

            // 根据获取的 RowKey，得到微博内容
            if (CollectionUtil.isEmpty(rowKeys)){
                return messages;
            }

            List<Get> contentGetList = new ArrayList<>();
            for (byte[] rk : rowKeys) {
                final Get tempGet = new Get(rk);
                contentGetList.add(tempGet);
            }
            final Result[] results = contentTable.get(contentGetList);
            for (Result res : results) {
                for (Cell cell : res.rawCells()) {
                    final Message message = new Message();

                    final String rk = Bytes.toString(CellUtil.cloneRow(cell));
                    final String[] attendIdAndTimestamp = rk.split("_");
                    final String attendId = attendIdAndTimestamp[0];
                    final String timestamp = attendIdAndTimestamp[1];
                    final String content = Bytes.toString(CellUtil.cloneValue(cell));

                    message.setUserId(attendId);
                    message.setTimestamp(timestamp);
                    message.setContent(content);
                    messages.add(message);
                }
            }

            return messages;
        }
    }
}
