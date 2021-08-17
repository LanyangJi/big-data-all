package cn.jly.bigdata.flink.scala.quickstart

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._

object D01_StreamWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)

    env.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .reduce(
        (t1, t2) => (t1._1, t1._2 + t2._2)
      )
      .print();

    env.execute("D01_StreamWordCount");
  }
}
