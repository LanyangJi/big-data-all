package cn.jly.bigdata.flink_advanced.utils;

import cn.jly.bigdata.flink_advanced.test.exer01.CategoryPojo;

import java.util.Comparator;
import java.util.Date;
import java.util.TreeSet;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.utils
 * @class TestMain
 * @date 2021/8/7 15:31
 */
public class TestMain {
    public static void main(String[] args) {
        TreeSet<CategoryPojo> set = new TreeSet<>(new Comparator<CategoryPojo>() {
            @Override
            public int compare(CategoryPojo o1, CategoryPojo o2) {
                return -Double.compare(o1.getTotalPrice(), o2.getTotalPrice());
            }
        });

        set.add(new CategoryPojo("t-1", 44d, new Date().toString()));
        set.add(new CategoryPojo("t-2", 55d, new Date().toString()));
        set.add(new CategoryPojo("t-3", 21d, new Date().toString()));
        set.add(new CategoryPojo("t-4", 33d, new Date().toString()));

        for (CategoryPojo categoryPojo : set) {
            System.out.println("categoryPojo = " + categoryPojo);
        }
    }
}
