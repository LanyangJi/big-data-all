package cn.spark.mocktest

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket


/**
 * @Author jilanyang
 * @Package cn.spark.mocktest
 * @Class MyDriver
 * @Date 2021/6/12 0012 16:35
 */
object MyDriver {
  def main(args: Array[String]): Unit = {
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    // 模拟Driver端向Executor发送计算逻辑（spark只发计算逻辑，这边为了简单模拟写死了数据在里面）
    val outputStream: OutputStream = client1.getOutputStream
    val objectOutputStream = new ObjectOutputStream(outputStream)
    // 发送计算逻辑
    val task = new Task
    // 计算拆分
    val subTask1 = new SubTask
    subTask1.datas = task.list.take(2)
    subTask1.logic = task.logic

    objectOutputStream.writeObject(subTask1);
    objectOutputStream.flush()
    objectOutputStream.close()
    client1.close()

    // 模拟Driver端向Executor发送计算逻辑（spark只发计算逻辑，这边为了简单模拟写死了数据在里面）
    val outputStream2: OutputStream = client2.getOutputStream
    val objectOutputStream2 = new ObjectOutputStream(outputStream2)
    // 发送计算逻辑
    // 计算拆分
    val subTask2 = new SubTask
    subTask2.datas = task.list.takeRight(2)
    subTask2.logic = task.logic

    objectOutputStream2.writeObject(subTask2);
    objectOutputStream2.flush()
    objectOutputStream2.close()
    client2.close()

    println("客户端发送完毕")
  }
}
