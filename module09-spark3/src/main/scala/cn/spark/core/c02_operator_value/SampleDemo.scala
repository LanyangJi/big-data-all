package cn.spark.core.c02_operator_value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 根据指定的规则从数据集中抽取数据
 *
 * // 抽取数据不放回（伯努利算法）
 * // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
 * // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
 * // 第一个参数：抽取的数据是否放回，false：不放回
 * // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
 * // 第三个参数：随机数种子
 * val dataRDD1 = dataRDD.sample(false, 0.5)
 * // 抽取数据放回（泊松算法）
 * // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
 * // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
 * // 第三个参数：随机数种子
 * val dataRDD2 = dataRDD.sample(true, 2)
 *
 * @Author jilanyang
 * @Package c02_transform_value
 * @Class SampleDemo
 * @Date 2021/5/31 0031 19:10
 */
object SampleDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SampleDemo"))
    sc.textFile("input/1*.txt")
      .flatMap(_.split(" "))
      /*
          第一个参数：抽取的数据是否放回，false不放回，true放回
          第二个参数：数据源中每条数据可能被抽取的概率
          第三个参数：抽取数据时随机算法的种子，随机种子
              注意：当你以确定的随机种子输入，则每次计算的随机值都是确定的，那么每次抽样的结果唯一

              一般随机种子可以选择不传递
       */
      .sample(false, 0.5)
      .collect()
      .foreach(println(_))

    sc.stop()
  }
}
