package cn.spark.mocktest

/**
 * @author Administrator
 * @date 2021/6/12 0012 18:45
 * @packageName cn.spark.mocktest 
 * @className SubTask
 */
class SubTask extends Serializable {
  var datas: List[Int] = _
  var logic: Int => Int = _

  def compute() = {
    datas.map(logic)
  }

  override def toString = s"Task($datas, $logic, $compute())"
}
