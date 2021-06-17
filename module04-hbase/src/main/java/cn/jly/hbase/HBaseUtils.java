package cn.jly.hbase;

import cn.hutool.core.lang.Console;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lanyangji
 * @date 2021/5/19 上午 11:19
 * @packageName cn.jly.hbase
 * @className HBaseUtils
 */
public class HBaseUtils {
    public static final Configuration CONF;

    static {
        CONF = HBaseConfiguration.create();
        CONF.set("hbase.master", "192.168.172.201:16000");
        CONF.set("hbase.zookeeper.quorum", "192.168.172.201");
        CONF.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 创建命名空间
     *
     * @param namespace
     * @param conf
     * @throws IOException
     */
    public static void createNamespace(String namespace, Map<String, String> conf) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(CONF);
             Admin admin = connection.getAdmin()) {
            NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).addConfiguration(conf).build();
            admin.createNamespace(descriptor);
        }
    }

    /**
     * 表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String tableName) throws IOException {
        // 在HBase中管理表、访问表需要创建HBaseAdmin对象
        try (Connection connection = ConnectionFactory.createConnection(CONF);
             Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    /**
     * 查询所有表名
     *
     * @return
     * @throws IOException
     */
    public static List<String> listTables() throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            return tableDescriptors.stream().map(t -> t.getTableName().getNameAsString()).collect(Collectors.toList());
        }
    }

    /**
     * 根据表名获得表描述符
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static TableDescriptor getTableDescriptorByTableName(String tableName) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                return null;
            }

            return admin.getDescriptor(tb);
        }
    }

    /**
     * 建表，如果存在则删除表
     *
     * @param admin
     * @param tableDescriptor
     * @throws Exception
     */
    private static void createOrOverwrite(Admin admin, TableDescriptor tableDescriptor) throws IOException {
        final TableName tableName = tableDescriptor.getTableName();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            Console.log("表{}已经存在，先删除", tableName.getNameAsString());
        }

        admin.createTable(tableDescriptor);
    }

    /**
     * 建表，如果表存在则覆盖表
     *
     * @param tableName      表名
     * @param columnFamilies 列族
     * @throws Exception
     */
    public static void createOrOverwriteTable(String tableName, String... columnFamilies) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            final TableDescriptorBuilder.ModifyableTableDescriptor descriptor
                    = (TableDescriptorBuilder.ModifyableTableDescriptor) TableDescriptorBuilder.newBuilder(tb).build();
            for (String columnFamily : columnFamilies) {
                final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnFamilyDescriptor =
                        new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes(columnFamily));
                descriptor.setColumnFamily(columnFamilyDescriptor);
            }

            Console.log("creating...{}", tableName);
            createOrOverwrite(admin, descriptor);
            Console.log("created... {}", tableName);
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws Exception
     */
    public static void dropTable(String tableName) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("表{}不存在", tableName);
                return;
            }

            Console.log("deleting... {}", tableName);
            admin.disableTable(tb);
            admin.deleteTable(tb);
            Console.log("deleted... {}", tableName);
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("表{}不存在", tableName);
                return;
            }

            try (final Table table = connection.getTable(tb)) {
                Console.log("adding row...{}", tableName);
                final Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                table.put(put);
                Console.log("added row...{}", tableName);
            }
        }
    }

    /**
     * 批量插入多行
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void addRows(String tableName, List<HBaseRow> rows) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("table {} does not exists...", tableName);
                return;
            }

            try (final Table table = connection.getTable(tb)) {
                final ArrayList<Put> puts = new ArrayList<>();

                for (HBaseRow row : rows) {
                    final String rowKey = row.getRowKey();
                    final Put put = new Put(Bytes.toBytes(rowKey));
                    for (Map.Entry<String, Map<String, String>> entry : row.getColumns().entrySet()) {
                        final String columnFamily = entry.getKey();
                        for (Map.Entry<String, String> entry2 : entry.getValue().entrySet()) {
                            final String columnName = entry2.getKey();
                            final String value = entry2.getValue();
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
                        }
                    }
                    puts.add(put);
                }

                table.put(puts);
                Console.log("added rowKey {} into table {}", rows.stream().map(HBaseRow::getRowKey).collect(Collectors.toList()), tableName);
            }
        }
    }

    /**
     * 删除多行数据
     *
     * @param tableName
     * @param rowKeys
     */
    public static void deleteRows(String tableName, String... rowKeys) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("table {} does not exists...", tableName);
                return;
            }

            try (final Table table = connection.getTable(tb)) {
                final List<Delete> deletes = Arrays.stream(rowKeys).map(rowKey -> new Delete(Bytes.toBytes(rowKey))).collect(Collectors.toList());
                table.delete(deletes);
                Console.log("deleted rowKeys {} from table {}", Arrays.toString(rowKeys), tableName);
            }
        }
    }

    /**
     * 获取所有的行
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<HBaseRow> getAllRows(String tableName) throws IOException {
        final ArrayList<HBaseRow> hBaseRows = new ArrayList<>();
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("table {} does not exists...", tableName);
                return hBaseRows;
            }

            try (final Table table = connection.getTable(tb)) {
                final Scan scan = new Scan();
                final ResultScanner scanner = table.getScanner(scan);
                getAndSetRows(hBaseRows, scanner);
                return hBaseRows;
            }
        }
    }

    /**
     * 封装结果数据
     *
     * @param hBaseRows
     * @param scanner
     */
    private static void getAndSetRows(List<HBaseRow> hBaseRows, Iterable<Result> scanner) {
        for (Result result : scanner) {
            final HBaseRow hBaseRow = new HBaseRow();
            hBaseRow.setRowKey(Bytes.toString(result.getRow()));
            hBaseRow.setColumns(new HashMap<>());

            final Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                final String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                final String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                final String value = Bytes.toString(CellUtil.cloneValue(cell));

                hBaseRow.getColumns().putIfAbsent(columnFamily, new HashMap<>());
                hBaseRow.getColumns().get(columnFamily).put(columnName, value);
            }

            hBaseRows.add(hBaseRow);
        }
    }

    /**
     * 获取单行或多行数据
     *
     * @param tableName
     * @param rowKeys
     * @return
     */
    public static List<HBaseRow> getRows(String tableName, String... rowKeys) throws IOException {
        final ArrayList<HBaseRow> hBaseRows = new ArrayList<>();
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("table {} does not exists...", tableName);
                return hBaseRows;
            }

            try (final Table table = connection.getTable(tb)) {
                final List<Get> gets = Arrays.stream(rowKeys).map(rowKey -> new Get(Bytes.toBytes(rowKey))).collect(Collectors.toList());
                final Result[] results = table.get(gets);
                getAndSetRows(hBaseRows, Arrays.asList(results));
                return hBaseRows;
            }
        }
    }

    /**
     * 清空一个或者多个表
     *
     * @param tableNames
     */
    public static void truncateTable(String... tableNames) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            for (String tableName : tableNames) {
                final TableName tb = TableName.valueOf(tableName);
                if (!admin.tableExists(tb)) {
                    Console.log("table {} does not exists...", tableName);
                    continue;
                }

                admin.disableTable(tb);
                Console.log("table {} disabled...", tableName);
                admin.truncateTable(tb, false);
                Console.log("table {} truncated...", tableName);
            }
        }
    }

    public static String getColumn(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        try (
                final Connection connection = ConnectionFactory.createConnection(CONF);
                final Admin admin = connection.getAdmin()
        ) {
            final TableName tb = TableName.valueOf(tableName);
            if (!admin.tableExists(tb)) {
                Console.log("table {} does not exists...", tableName);
                return null;
            }

            try (final Table table = connection.getTable(tb)) {
                final Get get = new Get(Bytes.toBytes(rowKey));
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
                final Result result = table.get(get);
                if (result.rawCells().length == 0) {
                    return null;
                }

                return Bytes.toString(CellUtil.cloneValue(result.rawCells()[0]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 校验表是否存在
        // System.out.println(isTableExists("student"));

        // 2. 建表
        // createOrOverwriteTable("student", "info", "info2");

        // 3. 删除表
        // dropTable("student");

        // 4. 查询所有表名
        // System.out.println(listTables());

        // 5. 根据表名查询表描述符
        // System.out.println(getTableDescriptorByTableName("student"));

        // 6. 插入一条记录
        // addRowData("student", "1001", "info", "name", "姬岚洋");

        // 7. 删除多行
        // deleteRows("student", "1001");

        // 8. 批量插入多行
        final ArrayList<HBaseRow> hBaseRows = new ArrayList<>();
        final HBaseRow hBaseRow = new HBaseRow();
        hBaseRow.setRowKey(String.valueOf(1001));
        hBaseRow.setColumns(new HashMap<>());
        hBaseRow.getColumns().putIfAbsent("info", new HashMap<>());
        hBaseRow.getColumns().get("info").put("name", "姬岚洋");
        hBaseRow.getColumns().get("info").put("age", "28");
        hBaseRow.getColumns().putIfAbsent("info2", new HashMap<>());
        hBaseRow.getColumns().get("info2").put("name", "杨晓华");
        hBaseRow.getColumns().get("info2").put("age", "27");

        final HBaseRow hBaseRow2 = new HBaseRow();
        hBaseRow2.setRowKey(String.valueOf(1002));
        hBaseRow2.setColumns(new HashMap<>());
        hBaseRow2.getColumns().putIfAbsent("info", new HashMap<>());
        hBaseRow2.getColumns().get("info").put("name", "张三");
        hBaseRow2.getColumns().get("info").put("age", "23");
        hBaseRow2.getColumns().putIfAbsent("info2", new HashMap<>());
        hBaseRow2.getColumns().get("info2").put("name", "李四");
        hBaseRow2.getColumns().get("info2").put("age", "22");

        hBaseRows.add(hBaseRow2);
        hBaseRows.add(hBaseRow);
//        addRows("student", hBaseRows);

        // 9. 清空表
        // truncateTable("student");

        // 10. 获取所有行数据
        //System.out.println(JSONUtil.toJsonPrettyStr(getAllRows("student")));

        // 11. 获取单行或者多行数据
        // System.out.println(JSONUtil.toJsonPrettyStr(getRows("student", "1002")));

        // 12. 获取某一列
        System.out.println(getColumn("student", "1002", "info", "name"));
    }
}
