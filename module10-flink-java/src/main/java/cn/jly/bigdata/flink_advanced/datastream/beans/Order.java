package cn.jly.bigdata.flink_advanced.datastream.beans;

import java.util.Objects;

/**
 * 订单类
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.beans
 * @class Order
 * @date 2021/7/29 22:31
 */
public class Order {
    private String orderId;
    private String userId;
    /**
     * event time
     */
    private Long createTime;
    private double money;

    public Order() {
    }

    public Order(String orderId, String userId, Long createTime, double money) {
        this.orderId = orderId;
        this.userId = userId;
        this.createTime = createTime;
        this.money = money;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + createTime +
                ", money=" + money +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Double.compare(order.money, money) == 0 && Objects.equals(orderId, order.orderId) && Objects.equals(userId, order.userId) && Objects.equals(createTime, order.createTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, createTime, money);
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }
}
