package com.github.inspalgo.core;

import java.util.Objects;

/**
 * @author InspAlgo
 * @date 2021/1/7 19:53 UTC+08:00
 */
public class Column implements Comparable<Column> {
    private String columnName;
    private Integer ordinalPosition;
    private String ddl;

    public Column() {
    }

    public String getColumnName() {
        return columnName;
    }

    public Column setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public Column setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
        return this;
    }

    public Column setOrdinalPosition(String ordinalPosition) {
        this.ordinalPosition = Integer.valueOf(ordinalPosition);
        return this;
    }

    public String getDdl() {
        return ddl;
    }

    public Column setDdl(String ddl) {
        this.ddl = ddl;
        return this;
    }

    @Override
    public int compareTo(Column o) {
        return ordinalPosition - o.ordinalPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return Objects.equals(ddl, column.ddl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }

    @Override
    public String toString() {
        return "Column{" +
            "columnName='" + columnName + '\'' +
            ", ordinalPosition=" + ordinalPosition +
            ", ddl='" + ddl + '\'' +
            '}';
    }
}
