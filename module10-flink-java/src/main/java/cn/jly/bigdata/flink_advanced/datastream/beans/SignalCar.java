package cn.jly.bigdata.flink_advanced.datastream.beans;

import java.util.Objects;

/**
 *记录通过信号灯的车的数量
 *
 * @author jilanyang
 * @date 2021/7/29 14:09
 * @package cn.jly.bigdata.flink_advanced.datastream.beans
 * @class SignalCar
 */
public class SignalCar {
    // 信号灯id
    private String signalId;
    // 通过信号灯的车的数量
    private Long passingCarCount;

    public SignalCar() {
    }

    public SignalCar(String signalId, Long passingCarCount) {
        this.signalId = signalId;
        this.passingCarCount = passingCarCount;
    }

    @Override
    public String toString() {
        return "SignalCar{" +
                "signalId='" + signalId + '\'' +
                ", passingCarCount=" + passingCarCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SignalCar signalCar = (SignalCar) o;
        return Objects.equals(signalId, signalCar.signalId) && Objects.equals(passingCarCount, signalCar.passingCarCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(signalId, passingCarCount);
    }

    public String getSignalId() {
        return signalId;
    }

    public void setSignalId(String signalId) {
        this.signalId = signalId;
    }

    public Long getPassingCarCount() {
        return passingCarCount;
    }

    public void setPassingCarCount(Long passingCarCount) {
        this.passingCarCount = passingCarCount;
    }
}
