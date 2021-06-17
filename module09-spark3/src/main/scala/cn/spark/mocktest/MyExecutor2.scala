package cn.spark.mocktest

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @Author jilanyang
 * @Package cn.spark.mocktest
 * @Class MyExecutor
 * @Date 2021/6/12 0012 16:35
 */
object MyExecutor2 {
  def main(args: Array[String]): Unit = {

    val server: ServerSocket = new ServerSocket(8888)
    println("服务器启动，等待接受数据")

    // 等待客户端的连接，接受Driver端发送过来的计算逻辑
    val client: Socket = server.accept()
    val inputStream: InputStream = client.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val task: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]
    println("接收到客户端发送的值：" + task)

    // 开始做计算
    val result: List[Int] = task.compute()
    println("计算结果为: " + result)

    objectInputStream.close()
    client.close()
    server.close()
  }
}
