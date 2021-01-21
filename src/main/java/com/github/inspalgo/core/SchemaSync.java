package com.github.inspalgo.core;

import java.util.ArrayList;
import java.util.List;

/**
 * @author InspAlgo
 * @date 2021/1/11 19:49 UTC+08:00
 */
public class SchemaSync {
    /**
     * 根据两张表的结构，生成可执行的 DDL 语句
     *
     * @param sourceTable 源表
     * @param targetTable 目标表
     * @return 目标表待执行的 DDL 语句
     */
    public static List<String> generateTableDdl(Table sourceTable, Table targetTable) {
        ArrayList<String> result = new ArrayList<>();
        String tableName = targetTable.getName();

        // 表的属性比对，如引擎、字符集等，表属性要同步要先执行
        // 先排除表属性中的自增主键同步，因为值的大小根据各个库实际数据大小定，要创建自增主键，只需指定好自增字段即可
        List<String> modifyAttributes = sourceTable.getAttributes();
        modifyAttributes.removeAll(targetTable.getAttributes());
        if (!modifyAttributes.isEmpty()) {
            result.add(String.format("ALTER TABLE `%s` %s", tableName, String.join(",", modifyAttributes)));
        }

        // 删除多余字段
        ArrayList<String> deleteColumnNames = targetTable.getAllColumnNames();
        deleteColumnNames.removeAll(sourceTable.getAllColumnNames());
        for (String columnName : deleteColumnNames) {
            result.add(String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, columnName));
        }

        // 判断增加/修改字段，同时保持字段相对顺序同步
        ArrayList<String> sourceColumnNames = sourceTable.getAllColumnNames();
        List<Column> sourceColumns = sourceTable.getColumns();
        ArrayList<String> targetColumnNames = targetTable.getAllColumnNames();
        // 要移除已删除字段，否则会导致字段顺序不同步
        targetColumnNames.removeAll(deleteColumnNames);

        String position = "FIRST";
        for (int i = 0, size = sourceColumnNames.size(); i < size; i++) {
            String sourceColumnName = sourceColumnNames.get(i);
            String sourceDdl = sourceColumns.get(i).getDdl();

            if (targetColumnNames.contains(sourceColumnName)) {
                // 字段位置不同步，需要注意只变动位置，要检测出最小化修改、避免影响多个字段位置修改
                // 先判断字段属性是否相同，因为属性不同的话，位置可以一并修改，然后再判断位置是否相同
                if (!sourceDdl.equals(targetTable.getColumnByName(sourceColumnName).getDdl())
                    || i != targetColumnNames.indexOf(sourceColumnName)) {
                    result.add(String.format("ALTER TABLE `%s` MODIFY COLUMN %s %s", tableName, sourceDdl, position));
                    insertColumn(targetColumnNames, sourceColumnName, i);
                }
            } else {
                result.add(String.format("ALTER TABLE `%s` ADD COLUMN %s %s", tableName, sourceDdl, position));
                insertColumn(targetColumnNames, sourceColumnName, i);
            }

            // 因为 AFTER 表示在指定字段之后，所以 position 的取值应在循环内的最后一步，避开了对第一个字段的特殊判断
            position = String.format("AFTER `%s`", sourceColumnNames.get(i));
        }

        // 主键同步
        String sourceTablePriKey = sourceTable.getPrimaryKey();
        String targetTablePriKey = targetTable.getPrimaryKey();
        if (sourceTablePriKey != null && targetTablePriKey == null) {
            result.add(String.format("ALTER TABLE `%s` ADD %s", tableName, sourceTablePriKey));
        } else if (sourceTablePriKey == null && targetTablePriKey != null) {
            result.add(String.format("ALTER TABLE `%s` DROP PRIMARY KEY", tableName));
        } else if (sourceTablePriKey != null && !sourceTablePriKey.equals(targetTablePriKey)) {
            result.add(String.format("ALTER TABLE `%s` DROP PRIMARY KEY, ADD %s", tableName, sourceTablePriKey));
        }

        // 索引同步
        List<String> deleteIndexes = targetTable.getIndexes();
        deleteIndexes.removeAll(sourceTable.getIndexes());
        for (String index : deleteIndexes) {
            result.add(String.format("ALTER TABLE `%s` DROP KEY %s",
                tableName, index.substring(index.indexOf('`'), index.indexOf('(')).trim()));
        }
        List<String> addIndexes = sourceTable.getIndexes();
        addIndexes.removeAll(targetTable.getIndexes());
        for (String index : addIndexes) {
            result.add(String.format("ALTER TABLE `%s` ADD %s", tableName, index));
        }

        return result;
    }

    private static void insertColumn(ArrayList<String> columnNames, String newColumnName, int insertPosition) {
        columnNames.remove(newColumnName);
        columnNames.add(insertPosition, newColumnName);
    }
}
