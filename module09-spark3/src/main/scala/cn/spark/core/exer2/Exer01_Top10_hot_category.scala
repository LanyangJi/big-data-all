package cn.spark.core.exer2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 需求 求 1 ：Top10  热门品类
 * 品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的
 * 公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。
 * 鞋    点击数 下单数 支付数
 * 衣服  点击数 下单数 支付数
 * 电脑  点击数 下单数 支付数
 * 例如，综合排名 = 点击数*20%+下单数*30%+支付数*50%
 * 本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
 *
 * @author Administrator
 * @date 2021/6/17 0017 11:14
 * @packageName cn.spark.core.exer2 
 * @className Exer01_Top10
 */
object Exer01_Top10_hot_category {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\devsoft\\hadoopBin")

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Exer01_Top10_hot_category")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 获取输入
    val inputRdd: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // 转换成userVisitAction对象
    val userVisitActionRdd: RDD[UserVisitAction] = inputRdd.map {
      line: String => {
        val fields: Array[String] = line.split("_")
        val date: String = fields(0)
        val user_id: Long = fields(1).trim.toLong
        val session_id: String = fields(2)
        val page_id: Long = fields(3).trim.toLong
        val action_time: String = fields(4)
        val search_keyword: String = if ("null".equals(fields(5).trim)) "" else fields(5).trim
        val click_category_id: Long = fields(6).trim.toLong
        val click_product_id: Long = fields(7).trim.toLong
        val order_category_ids: String = if ("null".equals(fields(8).trim)) "" else fields(8).trim
        val order_product_ids: String = if ("null".equals(fields(9).trim)) "" else fields(9).trim
        val pay_category_ids: String = if ("null".equals(fields(10).trim)) "" else fields(10).trim
        val pay_product_ids: String = if ("null".equals(fields(11).trim)) "" else fields(11).trim
        val city_id: Long = fields(12).trim.toLong

        UserVisitAction(date, user_id, session_id, page_id, action_time, search_keyword, click_category_id,
          click_product_id, order_category_ids, order_product_ids, pay_category_ids, pay_product_ids, city_id)
      }
    }

    // 缓存
    userVisitActionRdd.cache()

    println("---------------- 方案一：基于累加器的方式 ------------------")
    // 方案一：采用累加器的方式统计 （品类，点击总数，下单总数，支付总数）
    val accumulator = new CategoryAccumulator
    sc.register(accumulator)
    userVisitActionRdd.foreach(accumulator.add)
    // 排序并打印top10结果
    // 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    accumulator.value.values.toList.sortWith(sortCategoryCount)
      .take(10)
      .foreach(println)

    println("---------------- 方案二：直接统计 ------------------")
    // 方案二： 一次性统计成（category_id, （click_count, order_count, pay_count））的形式
    val countRdd: RDD[CategoryCount] = userVisitActionRdd.flatMap {
      action: UserVisitAction => { // 一个userVisitAction只能代表搜索、点击、下单、支付四种行为的一种
        val map: mutable.Map[String, CategoryCount] = mutable.Map[String, CategoryCount]()
        // 统计点击
        if (action.click_category_id != -1) {
          val categoryId: String = action.click_category_id.toString
          val newCount: CategoryCount = map.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
          newCount.click_count += 1
          map.update(categoryId, newCount)
        }
        // 统计下单
        if (action.order_category_ids.nonEmpty) {
          val ids: Array[String] = action.order_category_ids.split(",")
          for (categoryId <- ids) {
            val newCount: CategoryCount = map.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
            newCount.order_count += 1
            map.update(categoryId, newCount)
          }
        }
        // 统计支付
        if (action.pay_category_ids.nonEmpty) {
          val ids: Array[String] = action.pay_category_ids.split(",")
          for (categoryId <- ids) {
            val newCount: CategoryCount = map.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
            newCount.pay_count += 1
            map.update(categoryId, newCount)
          }
        }

        map
      }
    }
      .reduceByKey {
        (c1: CategoryCount, c2: CategoryCount) => {
          c1.click_count += c2.click_count
          c1.order_count += c2.order_count
          c1.pay_count += c2.pay_count
          c1
        }
      }
      .map(_._2)

    // 排序
    val counts: Array[CategoryCount] = countRdd.collect()
      .sortWith(sortCategoryCount)
      .take(10)
    counts.foreach(println)

    println("----------------- 需求2：Top10  热门品类中 每个品类的 的 Top10  活跃 Session  统计----------------")
    println("----------------- 需求2：在需求一的基础上，增加每个品类用户 session 的点击统计----------------")
    val topCategories: Array[String] = counts.map(_.category_id)
    // 可以直接在executor中使用topCategories（闭包检测）， 也可以使用广播变量
    val top_categories_broadcast: Broadcast[Array[String]] = sc.broadcast(topCategories)
    userVisitActionRdd.filter(action => top_categories_broadcast.value.contains(action.click_category_id.toString))
      .map(action => ((action.click_category_id, action.session_id), 1))
      .reduceByKey(_ + _)
      .sortBy(t => t._2, ascending = false)
      .take(10)
      .foreach(println)

    sc.stop()
  }

  /**
   * 排序函数
   *
   * @param c1
   * @param c2
   * @return
   */
  private def sortCategoryCount(c1: CategoryCount, c2: CategoryCount): Boolean = {
    var res: Int = c1.click_count.compareTo(c2.click_count)
    if (res == 0) {
      res = c1.order_count.compareTo(c2.order_count)
      if (res == 0) {
        res == c1.pay_count.compareTo(c2.pay_count)
      }
    }
    res > 0
  }

  // 品类统计样例类
  case class CategoryCount(var category_id: String, var click_count: Long, var order_count: Long, var pay_count: Long)

  /**
   * 自定义category累加器
   */
  class CategoryAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[String, CategoryCount]] {

    // 存储各种品类的统计信息
    private var categoryCountMap: mutable.Map[String, CategoryCount] = mutable.Map[String, CategoryCount]()

    /**
     * 初始状态
     *
     * @return
     */
    override def isZero: Boolean = this.categoryCountMap.isEmpty

    /**
     * 复制累加器
     *
     * @return
     */
    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[String, CategoryCount]] = {
      val accumulator = new CategoryAccumulator
      accumulator.categoryCountMap = this.categoryCountMap
      accumulator
    }

    /**
     * 重置累加器
     */
    override def reset(): Unit = this.categoryCountMap.clear()

    /**
     * 添加值
     *
     * @param v
     */
    override def add(newAction: UserVisitAction): Unit = {
      // 1. 统计点击行为次数
      if (newAction.click_category_id != -1) {
        val categoryId: String = newAction.click_category_id.toString
        val newCount: CategoryCount = this.categoryCountMap.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
        newCount.click_count += 1
        this.categoryCountMap.update(categoryId, newCount)
      }

      // 2. 统计下单行为次数
      if (newAction.order_category_ids.nonEmpty) {
        val ids: Array[String] = newAction.order_category_ids.split(",")
        for (categoryId <- ids) {
          val newCount: CategoryCount = this.categoryCountMap.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
          newCount.order_count += 1
          this.categoryCountMap.update(categoryId, newCount)
        }
      }

      // 3. 统计支付行为次数
      if (newAction.pay_category_ids.nonEmpty) {
        val ids: Array[String] = newAction.pay_category_ids.split(",")
        for (categoryId <- ids) {
          val newCount: CategoryCount = this.categoryCountMap.getOrElse(categoryId, CategoryCount(categoryId, 0, 0, 0))
          newCount.pay_count += 1
          this.categoryCountMap.update(categoryId, newCount)
        }
      }
    }

    /**
     * 累加器合并
     *
     * @param other
     */
    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[String, CategoryCount]]): Unit = {
      other match {
        case categoryAccumulator: CategoryAccumulator => {
          for ((k, v) <- categoryAccumulator.categoryCountMap) {
            val newCount: CategoryCount = this.categoryCountMap.getOrElse(k, CategoryCount(k, 0, 0, 0))
            newCount.click_count += v.click_count
            newCount.order_count += v.order_count
            newCount.pay_count += v.pay_count
            this.categoryCountMap.update(k, newCount)
          }
        }
      }
    }

    /**
     * 返回累加器的值
     *
     * @return
     */
    override def value: mutable.Map[String, CategoryCount] = this.categoryCountMap
  }
}
