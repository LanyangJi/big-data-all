package cn.spark.streaming.c06_exer.utils

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.mysql.jdbc.Driver

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

/**
 * @Author jilanyang
 * @Package cn.spark.streaming.c06_exer
 * @Class JdbcUtil
 * @Date 2021/6/10 0010 14:02
 */
object JdbcUtil {
  // 声明数据库连接池
  var datasource: DataSource = init()

  /**
   * 初始化数据库连接池
   *
   * @return
   */
  def init(): DataSource = {
    val properties = new Properties()
    // 加载配置
    val config: Properties = PropertyUtil.load("config.properties")
    properties.setProperty("driverClassName", classOf[Driver].getName)
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }


  /**
   * 获取数据库连接
   *
   * @return
   */
  def getConnection(): Connection = {
    datasource.getConnection
  }

  /**
   * 执行sql, 单条数据插入
   *
   * @param connection
   * @param sql
   * @param params
   * @return
   */
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var result = 0

    var preparedStatement: PreparedStatement = null
    try {
      // 关闭自动提交
      connection.setAutoCommit(false)
      preparedStatement = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (index <- params.indices) {
          preparedStatement.setObject(index + 1, params(index))
        }
      }
      result = preparedStatement.executeUpdate()
      connection.commit()

      // 日志
//      log(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
    }

    result
  }

  /**
   * 批量插入
   *
   * @param connection
   * @param sql
   * @param paramList
   * @return
   */
  def executeBatchUpdate(connection: Connection, sql: String, paramList: Iterable[Array[Any]]): Array[Int] = {
    var results: Array[Int] = null
    var preparedStatement: PreparedStatement = null

    try {
      // 关闭自动提交
      connection.setAutoCommit(false)
      preparedStatement = connection.prepareStatement(sql)
      for (params <- paramList) {
        if (params != null && params.length > 0) {
          for (index <- params.indices) {
            preparedStatement.setObject(index + 1, params(index))
          }
          preparedStatement.addBatch()
        }
      }
      results = preparedStatement.executeBatch()
      connection.commit()

      // 日志
//      log(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
    }

    results
  }

  /**
   * 判断一条数据是否存在
   *
   * @param connection
   * @param sql
   * @param params
   * @return
   */
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var preparedStatement: PreparedStatement = null

    try {
      preparedStatement = connection.prepareStatement(sql)
      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      flag = preparedStatement.executeQuery().next()

      // 日志
//      log(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
    }

    flag
  }

  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null

    try {
      preparedStatement = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (elem <- params.indices) {
          preparedStatement.setObject(elem + 1, params(elem))
        }
      }
      resultSet = preparedStatement.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }

      // 日志
//      log(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      if (resultSet != null)
        resultSet.close()
      if (preparedStatement != null)
        preparedStatement.close()
    }

    result
  }

  def log(sql: String): Unit = {
    println("执行的sql -> " + sql)
  }

  def main(args: Array[String]): Unit = {
    val l: Long = getDataFromMysql(getConnection(), "select count(*) from user_ad_count", null)
    println(l)
  }
}
