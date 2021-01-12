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
     * @param originTable 源表
     * @param targetTable 目标表
     * @return 目标表待执行的 DDL 语句
     */
    public static List<String> generateTableDdl(Table originTable, Table targetTable) {
        ArrayList<String> result = new ArrayList<>();
        String tableName = targetTable.getName();

        // 1. 删除多余字段
        ArrayList<String> deleteColumnNames = targetTable.getAllColumnNames();
        deleteColumnNames.removeAll(originTable.getAllColumnNames());
        for (String columnName : deleteColumnNames) {
            result.add(String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, columnName));
        }

        // 2. 判断增加/修改字段，同时保持字段相对顺序同步
        ArrayList<String> originColumnNames = originTable.getAllColumnNames();
        List<Column> originColumns = originTable.getColumns();
        ArrayList<String> targetColumnNames = targetTable.getAllColumnNames();

        String position = "FIRST";
        for (int i = 0, size = originColumnNames.size(); i < size; i++) {
            String originColumnName = originColumnNames.get(i);
            String originDdl = originColumns.get(i).getDdl();

            if (targetColumnNames.contains(originColumnName)) {
                // 字段位置不同步，需要注意只变动位置，要检测出最小化修改、避免影响多个字段位置修改
                // 先判断字段属性是否相同，因为属性不同的话，位置可以一并修改，然后再判断位置是否相同
                if (!originDdl.equals(targetTable.getColumnByName(originColumnName).getDdl())
                    || i != targetColumnNames.indexOf(originColumnName)) {
                    result.add(String.format("ALTER TABLE `%s` MODIFY COLUMN %s %s", tableName, originDdl, position));
                    insertColumn(targetColumnNames, originColumnName, i);
                }
            } else {
                result.add(String.format("ALTER TABLE `%s` ADD COLUMN %s %s", tableName, originDdl, position));
                insertColumn(targetColumnNames, originColumnName, i);
            }

            // 因为 AFTER 表示在指定字段之后，所以 position 的取值应在循环内的最后一步，避开了对第一个字段的特殊判断
            position = String.format("AFTER `%s`", originColumnNames.get(i));
        }

        // 3. 索引同步
        List<String> deleteIndexes = targetTable.getIndexes();
        deleteIndexes.removeAll(originTable.getIndexes());
        for (String index : deleteIndexes) {
            result.add(String.format("ALTER TABLE `%s` DROP %s", tableName, index.substring(0, index.indexOf('('))));
        }
        List<String> addIndexes = originTable.getIndexes();
        addIndexes.removeAll(targetTable.getIndexes());
        for (String index : addIndexes) {
            result.add(String.format("ALTER TABLE `%s` ADD %s", tableName, index));
        }

        // 4. 表的属性比对，如引擎、字符集等
        List<String> modifyAttributes = originTable.getAttributes();
        modifyAttributes.removeAll(targetTable.getAttributes());
        if (!modifyAttributes.isEmpty()) {
            result.add(String.format("ALTER TABLE `%s` %s", tableName, String.join(",", modifyAttributes)));
        }

        return result;
    }

    private static void insertColumn(ArrayList<String> columnNames, String newColumnName, int insertPosition) {
        columnNames.remove(newColumnName);
        columnNames.add(insertPosition, newColumnName);
    }
}
