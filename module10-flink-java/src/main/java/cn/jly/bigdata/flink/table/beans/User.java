package cn.jly.bigdata.flink.table.beans;

import java.time.Instant;
import java.util.Objects;

/**
 * @author jilanyang
 * @date 2021/7/20 17:25
 */
public class User {
    private String name;
    private Integer score;
    private Instant event_time;

    public User() {
    }

    public User(String name, Integer score, Instant event_time) {
        this.name = name;
        this.score = score;
        this.event_time = event_time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(name, user.name) && Objects.equals(score, user.score) && Objects.equals(event_time, user.event_time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, score, event_time);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Instant getEvent_time() {
        return event_time;
    }

    public void setEvent_time(Instant event_time) {
        this.event_time = event_time;
    }
}
