package cn.jly.bigdata.flink.table.beans;

/**
 * @author jilanyang
 * @date 2021/7/20 19:02
 */
public class RawUser {
    public final String name;
    public final Integer score;

    public RawUser(String name, Integer score) {
        this.name = name;
        this.score = score;
    }
}
