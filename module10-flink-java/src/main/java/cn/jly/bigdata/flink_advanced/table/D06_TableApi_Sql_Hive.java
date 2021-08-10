package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Apache Hive 已成为数据仓库生态系统的焦点。它不仅是一个用于大数据分析和 ETL 的 SQL 引擎，而且还是一个数据管理平台，
 * 用于发现、定义和演化数据。
 * Flink 提供了与 Hive 的双重集成。
 * 第一个是利用 Hive 的 Metastore 作为持久目录和 Flink 的 HiveCatalog 来跨会话存储 Flink 特定的元数据。例如，用户可以使用 HiveCatalog 将他们的 Kafka 或 ElasticSearch 表存储在 Hive Metastore 中，然后在 SQL 查询中重复使用它们。
 * 第二个是提供 Flink 作为读写 Hive 表的替代引擎（就是像MR/spark/Apache Tez那样作为hive的执行引擎）。
 * HiveCatalog 旨在与现有的 Hive 安装“开箱即用”兼容。您不需要修改现有的 Hive Metastore 或更改表的数据放置或分区。
 * <p>
 * 请注意，Hive 本身针对不同版本有不同的功能，这些问题并非由 Flink 引起：
 * 1.2.0 及更高版本支持 Hive 内置函数。
 * 3.1.0 及更高版本支持列约束，即 PRIMARY KEY 和 NOT NULL。
 * 1.2.0 及更高版本支持更改表统计信息。
 * 1.2.0 及更高版本支持 DATE 列统计信息。
 * 2.0.x 不支持写入 ORC 表。
 * <p>
 * 集成hive的步骤：
 * 要与Hive集成，需要在Flink发行版的/lib/目录下添加一些额外的依赖，使集成工作在Table API程序或SQL Client中的SQL中。或者，您可以将这些依赖项放在专用文件夹中，并分别使用 -C 或 -l 选项将它们添加到类路径中，用于表 API 程序或 SQL 客户端。
 * Apache Hive 是基于 Hadoop 构建的，因此您需要通过设置 HADOOP_CLASSPATH 环境变量来提供 Hadoop 依赖项：export HADOOP_CLASSPATH=`hadoop classpath`
 * <p>
 * 有两种方法可以添加 Hive 依赖项。首先是使用 Flink 捆绑的 Hive jars。您可以根据您使用的 Metastore 版本选择bundled Hive jar。
 * 其次是分别添加每个所需的jars。如果此处未列出您使用的 Hive 版本，则第二种方法可能很有用。
 * <p>
 * 下表列出了所有可用的捆绑 hive jar。您可以选择一个到 Flink 发行版中的 /lib/ 目录。
 * Metastore version	Maven dependency	        SQL Client JAR
 * 1.0.0 - 1.2.2	flink-sql-connector-hive-1.2.2	Download
 * 2.0.0 - 2.2.0	flink-sql-connector-hive-2.2.0	Download
 * 2.3.0 - 2.3.6	flink-sql-connector-hive-2.3.6	Download
 * 3.0.0 - 3.1.2	flink-sql-connector-hive-3.1.2	Download
 * <p>
 * 如果您正在构建自己的程序，则您的 mvn 文件中需要以下依赖项。建议不要在生成的 jar 文件中包含这些依赖项。您应该在运行时添加上述依赖项。
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-connector-hive_2.12</artifactId>
 *   <version>1.13.0</version>
 *   <scope>provided</scope>
 * </dependency>
 *
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-table-api-java-bridge_2.12</artifactId>
 *   <version>1.13.0</version>
 *   <scope>provided</scope>
 * </dependency>
 *
 * <dependency>
 *     <groupId>org.apache.hive</groupId>
 *     <artifactId>hive-exec</artifactId>
 *     <version>${hive.version}</version>
 *     <scope>provided</scope>
 * </dependency>
 *
 * 通过表环境或 YAML 配置，使用Catalog interface和 HiveCatalog 连接到现有的 Hive 安装。
 * 请注意，虽然 HiveCatalog 不需要特定的规划器，但读/写 Hive 表仅适用于blink规划器。因此，强烈建议您在连接到 Hive 仓库时使用blink计划器。
 * @author jilanyang
 * @date 2021/8/9 22:51
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D06_TableApi_Sql_Hive
 */
public class D06_TableApi_Sql_Hive {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir     = "/opt/hive-conf";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("hive", hiveCatalog);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

        // todo hive DDL DML
    }
}
