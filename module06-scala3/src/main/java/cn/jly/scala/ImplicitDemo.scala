package cn.jly.scala

/**
 * @Author jilanyang
 * @Package cn.jly.scala
 * @Class ImplicitDemo
 * @Date 2021/5/30 0030 14:05
 */
object ImplicitDemo {

  def main(args: Array[String]): Unit = {

    implicit val name: String = "lily"

    def test(implicit str: String = "tom"): Unit = {
      println(str)
    }


    test // lily
    test() // tom

    val sql = new MySql
    println(sql.addPrefix())
    println(sql.addSuffix())
  }


  implicit class AddChar(m: MySql) {
    def  addSuffix():String = {
      m + ".suffix"
    }

    def addPrefix(): String ={
      "prefix." + m
    }
  }
}
