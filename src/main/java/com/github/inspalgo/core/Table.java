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
    private final List<Column> columns = new ArrayList<>();
    private final List<String> indexes = new ArrayList<>();
    private String primaryKey = null;
    private String autoIncrement = null;
    private final List<String> attributes = new ArrayList<>();
    private String createTable;

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
        return createTable;
    }

    public String getNewCreateTable() {
        // 最后一个右括号，即 CREATE TABLE `` () 语句的 )
        int lastCloseBracket = createTable.lastIndexOf(')');
        StringBuilder sb = new StringBuilder(createTable);
        sb.delete(lastCloseBracket + 1, createTable.length()).append(" ");
        sb.append(String.join(" ", attributes));
        return sb.toString();
    }

    public void setCreateTable(String createTable) {
        this.createTable = createTable;
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

    public String getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(String autoIncrement) {
        this.autoIncrement = autoIncrement;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Table{" +
            "name='" + name + '\'' +
            ", columns=" + columns +
            ", indexes=" + indexes +
            ", attributes=" + attributes +
            '}';
    }
}
