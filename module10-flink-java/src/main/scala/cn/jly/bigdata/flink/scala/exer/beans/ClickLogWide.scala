package cn.jly.bigdata.flink.scala.exer.beans

/**
 * 点击日志宽表
 *
 * @author jilanyang
 * @date 2021/8/21 16:33
 */
case class ClickLogWide(
                         channelID: String, //频道ID
                         categoryID: String, //产品类别ID
                         produceID: String, //产品ID
                         country: String, //国家
                         province: String, //省份
                         city: String, //城市
                         network: String, //网络方式
                         source: String, //来源方式
                         browserType: String, //浏览器类型
                         entryTime: String, //进入网站时间
                         leaveTime: String, //离开网站时间
                         userID: String, //用户的ID

                         count: Long, // 用户访问次数
                         timestamp: Long, // 用户访问时间
                         address: String, // 国家-省份-城市 拼接
                         yearMonth: String, // 年月
                         yearMonthDay: String, //年月日
                         yearMonthDayHour: String, // 年月日小时
                         isNew: Int, //是否为访问某个频道的新用户——0表示否，1表示是
                         isHourNew: Int, //在某一小时内是否为某个频道的新用户——0表示否，1表示是
                         isDayNew: Int, //在某一天是否为某个频道的新用户—0表示否，1表示是
                         isMonthNew: Int //在某一个月是否为某个频道的新用户——0表示否，1表示是
                       )
