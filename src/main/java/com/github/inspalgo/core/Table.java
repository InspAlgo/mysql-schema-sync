package com.github.inspalgo.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author InspAlgo
 * @date 2021/1/7 20:05 UTC+08:00
 */
public class Table {
    private String name;
    private String engine = null;
    private String charset = null;
    private String primaryKey = null;
    private String autoIncrement = null;
    private String rowFormat = null;
    private final List<Column> columns = new ArrayList<>();
    private final List<String> indexes = new ArrayList<>();
    private final List<String> attributes = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public String getCreateTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `").append(name).append("` (");
        for (int i = 0, size = columns.size(); i < size; i++) {
            sb.append(" ").append(columns.get(i).getDdl());
            if (i < size - 1) {
                sb.append(",");
            }
        }
        if (primaryKey != null) {
            sb.append(", ").append(primaryKey);
        }
        for (String index : indexes) {
            sb.append(", ").append(index);
        }
        sb.append(" ) ENGINE=").append(engine).append(" CHARACTER SET=").append(charset)
          .append(" ").append(String.join(" ", attributes));
        if (rowFormat != null) {
            sb.append(" ROW_FORMAT=").append(rowFormat);
        }
        return sb.toString();
    }

    public ArrayList<String> getAllColumnNames() {
        ArrayList<String> result = new ArrayList<>(columns.size());
        columns.forEach(c -> result.add(c.getColumnName()));
        return result;
    }

    public Column getColumnByName(String columnName) {
        for (Column column : columns) {
            if (column.getColumnName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    public List<String> getIndexes() {
        ArrayList<String> result = new ArrayList<>(indexes.size());
        result.addAll(indexes);
        return result;
    }

    public void addIndex(String index) {
        indexes.add(index);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(String autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getRowFormat() {
        return rowFormat;
    }

    public void setRowFormat(String rowFormat) {
        this.rowFormat = rowFormat.toUpperCase();
    }

    public List<String> getAttributes() {
        ArrayList<String> result = new ArrayList<>(attributes.size());
        result.addAll(attributes);
        return result;
    }

    public void addAttribute(String attribute) {
        if (attribute == null || attribute.isEmpty()) {
            return;
        }
        attributes.add(attribute);
    }

    /**
     * 数据自检
     *
     * @return true-数据正常；false-数据存在异常，如缺少 ENGINE、CHARACTER SET 等关键数据
     */
    public boolean selfCheck() {
        String[] properties = new String[]{name, engine, charset};
        for (String property : properties) {
            if (property == null || property.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        if (table.name == null || table.name.isEmpty()) {
            return false;
        }
        if (name == null || name.isEmpty()) {
            return false;
        }
        return name.equals(table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Table{" +
            "name='" + name + '\'' +
            ", engine='" + engine + '\'' +
            ", charset='" + charset + '\'' +
            ", primaryKey='" + primaryKey + '\'' +
            ", autoIncrement='" + autoIncrement + '\'' +
            ", rowFormat='" + rowFormat + '\'' +
            ", columns=" + columns +
            ", indexes=" + indexes +
            ", attributes=" + attributes +
            '}';
    }
}
