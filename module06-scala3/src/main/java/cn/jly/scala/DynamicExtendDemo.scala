package cn.jly.scala

/**
 * @author jilanyang
 * @date 2021/5/30 0030 10:31
 * @packageName cn.jly.scala 
 * @className DynamicExtend
 */
object DynamicExtendDemo {
  def main(args: Array[String]): Unit = {
    val mySql: MySql with Operate = new MySql with Operate {
      override def hello(): String = {
        val str: String = "hello"
        println(str)
        str
      }
    }

    mySql.insert()
    val res: String = mySql.hello()
    println(s"str = $res")
  }
}

trait Operate {
  def insert(): Unit = {
    println("operate insert")
  }

  def hello(): String
}

class MySql