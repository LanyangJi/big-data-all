package cn.spark.core.exer2

/**
 * 用户访问行为描述基类
 *
 * 上面的数据图是从数据文件中截取的一部分内容，表示为电商网站的用户行为数据，主
 * 要包含用户的 4 种行为：搜索，点击，下单，支付。数据规则如下：
 * ➢  数据文件中每行数据采用下划线分隔数据
 * ➢  每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
 * ➢  如果搜索关键字为 null,表示数据不是搜索数据
 * ➢  如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
 * ➢  针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个，id 之
 * 间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
 * ➢  支付行为和下单行为类似
 *
 * @author jilanyang
 * @date 2021/6/17 0017 11:06
 * @packageName cn.spark.core.exer2 
 * @className UserVisitAction
 */
case class UserVisitAction(
                            date: String, // 日期
                            user_id: Long, // 用户id
                            session_id: String, // 会话id
                            page_id: Long, // 页面id
                            action_time: String, // 动作的详细时间点
                            search_word: String, // 用户搜索的关键字
                            click_category_id: Long, // 某一种商品品类的id
                            click_product_id: Long, // 某一个商品的id
                            order_category_ids: String, // 一次订单中所有商品品类的id的集合
                            order_product_ids: String, // 一次订单中所有商品的id的集合
                            pay_category_ids: String, // 一次支付中所有商品品类的id的集合
                            pay_product_ids: String, // 一次支付中所有商品的id的集合
                            city_id: Long // 城市id
                          )
