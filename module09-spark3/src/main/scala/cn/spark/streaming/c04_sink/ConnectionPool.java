package cn.spark.streaming.c04_sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 自定义数据库连接池
 *
 * @Author jilanyang
 * @Package cn.spark.streaming.c04_sink
 * @Class ConnectionPool
 * @Date 2021/6/9 0009 14:06
 */
public class ConnectionPool {
    private ConnectionPool() {
    }

    private static LinkedList<Connection> connectionQueue;
    private static final String JDBC_URL = "jdbc:mysql://linux01:3306/test";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < 15; i++) {
                    Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    connectionQueue.push(connection);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * 释放连接回连接池
     *
     * @param connection
     */
    public static void returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }
}
