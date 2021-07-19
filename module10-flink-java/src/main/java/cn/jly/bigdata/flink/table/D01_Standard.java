package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * table api和 sql的标准结构
 * <p>
 * 一、表环境TableEnvironment
 * TableEnvironment是Table API和SQL集成的入口点，负责：
 * 1. 在内部catalog中注册表
 * 2. 注册catalog
 * 3. 加载可插拔模块
 * 4. 执行SQL查询
 * 5. 注册用户定义(标量、表或聚合)函数
 * 6. DataStream和Table之间的转换(对于StreamTableEnvironment)
 * <p>
 * 一个表总是绑定到一个特定的TableEnvironment。不可能在同一个查询中组合不同TableEnvironments的表，
 * 例如，联接或联合它们。TableEnvironment是通过调用静态的TableEnvironment.create()方法来创建的。
 * <p>
 * 或者，用户可以从现有的StreamExecutionEnvironment创建一个StreamTableEnvironment来与DataStream API互操作。
 * <p>
 * 二、表
 * TableEnvironment维护一个表的catalog映射，这些表是用一个标识符创建的。每个标识符由3部分组成:catalog、数据库名称和对象名称。
 * 如果没有指定catalog或数据库，将使用当前的默认值(参见表标识符展开部分中的示例)。
 * <p>
 * 表可以是virtual (VIEWS)或regular (Tables)。视图可以从现有的Table对象创建，通常是Table API或SQL查询的结果。
 * TABLES描述外部数据，如文件、数据库表或消息队列。
 * <p>
 * 三、临时表 vs 永久表
 * 表可以是临时的，绑定到单个Flink会话的生命周期，也可以是永久的，跨多个Flink会话和集群可见。
 * <p>
 * 1. 永久表需要一个catalog(如Hive Metastore)来维护关于表的元数据。一旦创建了永久表，它对任何连接到catalog的Flink会话都是可见的，并且将继续存在，直到显式地删除表。
 * 2. 另一方面，临时表总是存储在内存中，并且只在创建临时表的Flink会话期间存在。这些表对其他会话不可见。它们没有绑定到任何catalog或数据库，但可以在其中一个名称空间中创建。
 * 如果临时表对应的数据库被删除，则不会删除临时表。
 * <p>
 * 四、遮挡（优先级，同名情况下，本次会还中临时表优先级更高）
 * 1. 可以使用与现有永久表相同的标识符注册临时表。临时表遮蔽永久表，只要临时表存在，永久表就不可访问。所有具有该标识符的查询都将针对临时表执行。
 * <p>
 * 五、建表
 * 1. 一个Table API对象对应于一个SQL术语中的VIEW(虚拟表)。它封装了一个逻辑查询计划
 * // get a TableEnvironment
 * TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section
 * <p>
 * // table is the result of a simple projection query
 * Table projTable = tableEnv.from("X").select(...);
 * <p>
 * // register the Table projTable as table "projectedTable"
 * tableEnv.createTemporaryView("projectedTable", projTable);
 * <p>
 * 2. 查询视图结果不共享
 * Table对象类似于关系数据库系统中的VIEW，也就是说，定义Table的查询没有被优化，但是当另一个查询引用已注册的Table时，它将被内联。
 * 如果多个查询引用相同的注册表，它将内联每个引用查询，并执行多次，也就是说，注册表的结果将不会被共享。
 * <p>
 * 六、表标志符
 * 表总是使用一个由catalog、数据库和表名组成的标识符进行注册。
 * -->
 * TableEnvironment tEnv = ...;
 * tEnv.useCatalog("custom_catalog");
 * tEnv.useDatabase("custom_database");
 * <p>
 * Table table = ...;
 * <p>
 * // register the view named 'exampleView' in the catalog named 'custom_catalog'
 * // in the database named 'custom_database'
 * tableEnv.createTemporaryView("exampleView", table);
 * <p>
 * // register the view named 'exampleView' in the catalog named 'custom_catalog'
 * // in the database named 'other_database'
 * tableEnv.createTemporaryView("other_database.exampleView", table);
 * <p>
 * // register the view named 'example.View' in the catalog named 'custom_catalog'
 * // in the database named 'custom_database'
 * tableEnv.createTemporaryView("`example.View`", table);
 * <p>
 * // register the view named 'exampleView' in the catalog named 'other_catalog'
 * // in the database named 'other_database'
 * tableEnv.createTemporaryView("other_catalog.other_database.exampleView", table);
 * -->
 * <p>
 * 用户可以将其中的一个catalog和一个数据库设置为当前catalog和当前数据库。使用它们，上面提到的3部分标识符中的前两个部分可以是可选的——如果没有提供它们，
 * 则将引用当前catalog和当前数据库。用户可以通过表API或SQL切换当前catalog和当前数据库。
 * 标识符遵循SQL要求，这意味着它们可以用反勾字符(')进行转义
 * <p>
 * 七、Table API
 * Table API是一个用于Scala和Java的语言集成查询API。与SQL相反，查询没有指定为string，而是使用宿主语言一步一步地组成。
 * <p>
 * 该API基于Table类，它表示一个表(流或批处理)，并提供了应用关系操作的方法。这些方法返回一个新的Table对象，
 * 该对象表示对输入Table应用关系操作的结果。一些关系操作由多个方法调用组成，如table.groupBy(…).select()，
 * 其中groupBy(…)指定一个表的分组，并选择(…)在这个表的分组上的投影。
 * --->
 * // get a TableEnvironment
 * TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section
 * <p>
 * // register Orders table
 * <p>
 * // scan registered Orders table
 * Table orders = tableEnv.from("Orders");
 * // compute revenue for all customers from France
 * Table revenue = orders
 * .filter($("cCountry").isEqual("FRANCE"))
 * .groupBy($("cID"), $("cName"))
 * .select($("cID"), $("cName"), $("revenue").sum().as("revSum"));
 * <p>
 * // emit or convert Table
 * // execute query
 * --->
 * <p>
 * 八、SQL
 * Flink的SQL集成基于Apache Calcite，实现了SQL标准。SQL查询被指定为常规字符串。SQL文档描述了Flink对流和批处理表的SQL支持。
 * --->
 * // get a TableEnvironment
 * TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section
 * <p>
 * // register Orders table
 * <p>
 * // compute revenue for all customers from France
 * Table revenue = tableEnv.sqlQuery(
 * "SELECT cID, cName, SUM(revenue) AS revSum " +
 * "FROM Orders " +
 * "WHERE cCountry = 'FRANCE' " +
 * "GROUP BY cID, cName"
 * );
 * <p>
 * // emit or convert Table
 * // execute query
 * --->
 * // get a TableEnvironment
 * TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section
 * <p>
 * // register "Orders" table
 * // register "RevenueFrance" output table
 * <p>
 * // compute revenue for all customers from France and emit to "RevenueFrance"
 * tableEnv.executeSql(
 * "INSERT INTO RevenueFrance " +
 * "SELECT cID, cName, SUM(revenue) AS revSum " +
 * "FROM Orders " +
 * "WHERE cCountry = 'FRANCE' " +
 * "GROUP BY cID, cName"
 * );
 * --->
 * <p>
 * 九、转换和执行查询
 * 两个计划器转换和执行查询的行为是不同的。表API和SQL查询被转换成DataStream程序，不管它们的输入是流的还是批处理的。查询在内部表示为逻辑查询计划，并分为两个阶段:
 * 1。优化逻辑计划，
 * 2。转换成DataStream程序
 * <p>
 * 转换表API或SQL查询时
 * 1. TableEnvironment.executeSql()。此方法用于执行给定的语句，调用此方法后立即转换sql查询。
 * 2. Table.executeInsert()。此方法用于将表内容插入到给定的接收器路径，并且在调用此方法后立即转换table API。
 * 3. Table.execute()。此方法用于将表内容收集到本地客户机，并且在调用此方法后立即转换table API。
 * 4. StatementSet.execute()。Table(通过StatementSet. addinsert()发送到接收器)或INSERT语句(通过StatementSet. addinsertsql()指定)将首先在StatementSet中缓冲。
 * 一旦调用StatementSet.execute()，它们就会被转换。所有的sink将被优化为一个DAG。
 * 5. 当一个表被转换为一个DataStream时，它就会被转换(参见与DataStream集成)。转换后，它将成为一个常规的DataStream程序，
 * 并在调用StreamExecutionEnvironment.execute()时执行。
 * <p>
 * 十、查询优化
 * Apache Flink利用并扩展了Apache Calcite来执行复杂的查询优化。这包括一系列基于规则和成本的优化，例如
 * 1 基于Apache方解石的子查询去关联
 * 2 项目修剪
 * 3 分区修剪
 * 4 过滤器下推
 * 5 重复数据删除分规划，避免重复计算
 * 6 特殊的子查询重写，包括两部分:
 * 将IN和EXISTS转换为左半连接
 * 将NOT IN和NOT EXISTS转换为左反连接
 * 7 可选加入重新排序，通过启用table.optimizer.join-reorder-enabled
 * <p>
 * 优化器不仅基于计划，而且基于来自数据源的丰富统计数据和每个操作符(如io、cpu、网络和内存)的细粒度成本来做出智能决策。
 * 高级用户可以通过调用TableEnvironment#getConfig#setPlannerConfig提供给表环境的CalciteConfig对象来提供自定义优化。
 * <p>
 * 十一、explain table
 * Table API提供了一种机制来解释计算Table的逻辑和优化查询计划。这是通过Table.explain()方法或StatementSet.explain()方法完成的。
 * Table.explain()返回Table的计划。StatementSet.explain()返回多个接收器的计划。它返回一个描述三个计划的String
 * -> 关系查询的抽象语法树，即未优化的逻辑查询计划、优化的逻辑查询计划和物理执行计划
 *TableEnvironment.explainSql()和TableEnvironment.executeSql()支持执行EXPLAIN语句来获取计划，请参考EXPLAIN页面
 * @author jilanyang
 * @date 2021/7/15 21:48
 */
public class D01_Standard {
    public static void main(String[] args) {

        // 1. 获取表执行环境
        // 方式一
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 方式二
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建输入表
        // tableEnv.executeSql("CREATE TEMPORARY TABLE inputTable ... WITH ( 'connector' = ... )");

        // 3. 创建输出表
        // tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");

        // 4. 创建table对象，并使用table api或者sql操作表
        // Table API query
        // Table table2 = tableEnv.from("inputTable").select(...);
        // SQL query
        // Table table3 = tableEnv.sqlQuery("SELECT ... FROM inputTable ... ");

        // 5. 输出
        // TableResult tableResult = table2.executeInsert("outputTable");
        // tableResult...
    }
}
