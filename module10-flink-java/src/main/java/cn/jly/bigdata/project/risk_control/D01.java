package cn.jly.bigdata.project.risk_control;

/**
 * 电商用户行为分析案例
 * <p>
 * 1. 实时统计分析
 * - 实时热门商品统计
 * - 实时热门页面流量统计
 * - 实时访问流量统计
 * - app市场推广统计
 * - 页面广告点击量统计
 * <p>
 * 2. 业务流程及风险控制
 * - 页面广告黑名单过滤
 * - 恶意登录监控
 * - 订单支付实现监控
 * - 支付实时对账
 *
 * 数据源：用户行为UserBehavior.csv
 * userId           Long        用户id
 * itemId           Long        商品id
 * categoryId       Integer     品类id
 * behavior         String      行为 pv(浏览)、buy(购买)、cart(加入购物车)、fav(喜欢)
 * timestamp        Long        行为发生的时间戳，单位秒
 *
 * @author jilanyang
 * @date 2021/8/29 11:52
 */
public class D01 {
}
