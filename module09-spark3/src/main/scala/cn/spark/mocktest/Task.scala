package cn.spark.mocktest

/**
 * 包含数据和计算逻辑，其实这里扮演的是数据结构的作用。
 * 会在driver端被拆分
 *
 * @author Administrator
 * @date 2021/6/12 0012 17:11
 * @packageName cn.spark.mocktest 
 * @className Task
 */
class Task extends Serializable {
  val list = List(1, 2, 3, 4)

  val logic = (num: Int) => num * 2
}
