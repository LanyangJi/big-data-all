package cn.jly.bigdata.flink.datastream.beans;

/**
 * @author jilanyang
 * @date 2021/6/30 0030 13:15
 * @packageName cn.jly.bigdata.flink.datastream.beans
 * @className SensorReading
 */
public class SensorReading {
    private String name;
    private Long timestamp;
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String name, Long timestamp, Double temperature) {
        this.name = name;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
