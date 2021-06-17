package cn.jly.hbase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author lanyangji
 * @date 2021/5/20 下午 12:49
 * @packageName cn.jly.hbase
 * @className Row
 */
public class HBaseRow implements Serializable {

    /**
     * rowKey
     */
    private String rowKey;
    /**
     * key1: 列族
     * key2: 列名
     * value: 列值
     */
    private Map<String, Map<String, String>> columns = new HashMap<>();

    public HBaseRow() {
    }

    public HBaseRow(String rowKey, Map<String, Map<String, String>> columns) {
        this.rowKey = rowKey;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "HBaseRow{" +
                "rowKey='" + rowKey + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HBaseRow hBaseRow = (HBaseRow) o;
        return Objects.equals(rowKey, hBaseRow.rowKey) && Objects.equals(columns, hBaseRow.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKey, columns);
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, Map<String, String>> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Map<String, String>> columns) {
        this.columns = columns;
    }
}
