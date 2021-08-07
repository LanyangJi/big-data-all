package cn.jly.bigdata.flink_advanced.test.exer01;

import java.util.Objects;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class CategoryPojo
 * @date 2021/8/4 21:57
 */
public class CategoryPojo {
    // 品类名称
    private String category;
    // 品类销售总额
    private Double totalPrice;
    // 截止到当前时间的时间，本来应该是event time,但是这里为了简化直接使用当前系统时间即可
    private String dateTime;

    public CategoryPojo() {
    }

    public CategoryPojo(String category, Double totalPrice, String dateTime) {
        this.category = category;
        this.totalPrice = totalPrice;
        this.dateTime = dateTime;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(Double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategoryPojo that = (CategoryPojo) o;
        return Objects.equals(category, that.category) && Objects.equals(totalPrice, that.totalPrice) && Objects.equals(dateTime, that.dateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, totalPrice, dateTime);
    }

    @Override
    public String toString() {
        return "CategoryPojo{" +
                "category='" + category + '\'' +
                ", totalPrice=" + totalPrice +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }
}
