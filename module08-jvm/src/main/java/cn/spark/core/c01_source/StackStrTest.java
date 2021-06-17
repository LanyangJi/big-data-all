package cn.spark.core.c01_source;

/**
 * javap -v StackStrTest.class
 *
 * jps
 *
 * @author jilanyang
 * @date 2021/5/29 0029 17:00
 * @packageName c01
 * @className StackStrTest
 */
public class StackStrTest {
    public static void main(String[] args) {
        int i = 2;
        int j = 3;
        int k = i + j;

        try {
            Thread.sleep(6000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("hello");
    }
}
