package cn.jly.bigdata.project.beans;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author jilanyang
 * @date 2021/9/4 10:14
 */
public class PageViewCount implements Serializable {
    private String url;
    private Long windowEnd;
    private Long count;

    public PageViewCount() {
    }

    public PageViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewCount{" + "url='" + url + '\'' + ", windowEnd=" + windowEnd + ", count=" + count + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PageViewCount that = (PageViewCount) o;
        return Objects.equals(url, that.url) && Objects.equals(windowEnd, that.windowEnd) && Objects.equals(count,
                that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, windowEnd, count);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
