package cn.jly.bigdata.project.beans;

import java.io.Serializable;
import java.util.Objects;

/**
 * 日志事件bean
 *
 * @author jilanyang
 * @date 2021/9/4 10:11
 */
public class ApacheLogEvent implements Serializable {
	private String ip;
	private String userId;
	private Long timestamp;
	private String method;
	private String url;

	public ApacheLogEvent() {
	}

	public ApacheLogEvent(String ip, String userId, Long timestamp, String method, String url) {
		this.ip = ip;
		this.userId = userId;
		this.timestamp = timestamp;
		this.method = method;
		this.url = url;
	}

	@Override
	public String toString() {
		return "ApacheLogEvent{" + "ip='" + ip + '\'' + ", userId='" + userId + '\'' + ", timestamp=" + timestamp
				+ ", method='" + method + '\'' + ", url='" + url + '\'' + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ApacheLogEvent that = (ApacheLogEvent) o;
		return Objects.equals(ip, that.ip) && Objects.equals(userId, that.userId) && Objects.equals(timestamp,
				that.timestamp) && Objects.equals(method, that.method) && Objects.equals(url, that.url);
	}

	@Override
	public int hashCode() {
		return Objects.hash(ip, userId, timestamp, method, url);
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
