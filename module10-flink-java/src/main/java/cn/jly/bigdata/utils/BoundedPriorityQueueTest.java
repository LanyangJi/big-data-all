package cn.jly.bigdata.utils;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import cn.jly.bigdata.flink_advanced.test.exer01.WindowFinalProcessFunction;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * 小顶堆
 * <p>
 * 举个例子。我有一个用户表，这个表根据用户名被Hash到不同的数据库实例上，我要找出这些用户中最热门的5个，怎么做？我是这么做的：
 * <p>
 * 在每个数据库实例上找出最热门的5个
 * 将每个数据库实例上的这5条数据按照热门程度排序，最后取出前5条
 * 这个过程看似简单，但是你应用服务器上的代码要写不少。首先需要Query N个列表，加入到一个新列表中，排序，再取前5。这个过程不但代码繁琐，而且牵涉到多个列表，非常浪费空间。
 * <p>
 * 于是，BoundedPriorityQueue应运而生。
 * <p>
 * {@link WindowFinalProcessFunction} 中的业务场景就用到了这个工具类描述的场景，
 * 但是那边是自己封装的
 *
 * @author jilanyang
 * @createTime 2021/8/13 17:20
 */
public class BoundedPriorityQueueTest {
    public static void main(String[] args) {

        // 初始化队列，使用自定义的比较器
        BoundedPriorityQueue<Order> boundedPriorityQueue = new BoundedPriorityQueue<>(
                3,
                new Comparator<Order>() {
                    @Override
                    public int compare(Order o1, Order o2) {
                        return -Double.compare(o1.getMoney(), o2.getMoney());
                    }
                }
        );

        ArrayList<Order> list = new ArrayList<>();
        list.add(new Order("order-1", "user-1", 1000L, 23d));
        list.add(new Order("order-2", "user-2", 2000L, 44d));
        list.add(new Order("order-3", "user-3", 3000L, 55d));
        list.add(new Order("order-4", "user-4", 4000L, 21d));
        list.add(new Order("order-5", "user-5", 5000L, 66d));
        list.add(new Order("order-6", "user-6", 6000L, 12d));

        boundedPriorityQueue.addAll(list);

        ArrayList<Order> orders = boundedPriorityQueue.toList();
        orders.forEach(System.out::println);
    }
}
