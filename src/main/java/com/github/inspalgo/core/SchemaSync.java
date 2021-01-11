package com.github.inspalgo.core;

import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author InspAlgo
 * @date 2021/1/11 19:49 UTC+08:00
 */
public class SchemaSync {

    private ThreadPoolExecutor executor = TableThreadPoolExecutor.make("SchemaSync");

    public SchemaSync() {

    }

    /**
     * 根据两张表的结构，生成可执行的 DDL 语句
     *
     * @param originTable 源表
     * @param targetTable 目标表
     * @return 目标表待执行的 DDL 语句
     */
    public List<String> generateTableDdl(Table originTable, Table targetTable) {
        ArrayList<String> result = new ArrayList<>();
        String tableName = targetTable.getName();

        // 1. 删除多余字段
        ArrayList<String> deleteColumnNames = targetTable.getAllColumnNames();
        deleteColumnNames.removeAll(originTable.getAllColumnNames());
        for (String columnName : deleteColumnNames) {
            String ddl = String.format("ALTER TABLE `%s` DROP COLUMN `%s`;", tableName, columnName);
            result.add(ddl);
        }

        // 2. 判断增加/修改字段
        ArrayList<String> originColumnNames = originTable.getAllColumnNames();
        List<Column> originColumns = originTable.getColumns();
        ArrayList<String> targetColumnNames = targetTable.getAllColumnNames();
        List<Column> targetColumns = targetTable.getColumns();

        // 第一个字段要特殊判断
        String originColumnName = originColumnNames.get(0);
        String originDdl = originColumns.get(0).getDdl();
        if (targetColumnNames.contains(originColumnName)) {
            if (!originDdl.equals(targetColumns.get(0).getDdl())) {
                String ddl = String.format("ALTER TABLE `%s` MODIFY COLUMN %s FIRST;", tableName, originDdl);
                result.add(ddl);
            }
        } else {
            String ddl = String.format("ALTER TABLE `%s` ADD COLUMN %s FIRST;", tableName, originDdl);
            result.add(ddl);
        }
        // 其余字段
        for (int i = 1, size = originColumnNames.size(); i < size; i++) {
            originColumnName = originColumnNames.get(i);
            originDdl = originColumns.get(i).getDdl();
            if (targetColumnNames.contains(originColumnName)) {
                if (!originDdl.equals(targetColumns.get(i).getDdl())) {
                    String ddl = String.format("ALTER TABLE `%s` MODIFY COLUMN %s AFTER `%s`;",
                        tableName, originDdl, originColumnNames.get(i - 1));
                    result.add(ddl);
                }
            } else {
                String ddl = String.format("ALTER TABLE `%s` ADD COLUMN %s AFTER `%s`;",
                    tableName, originDdl, originColumnNames.get(i - 1));
                result.add(ddl);
            }
        }

        // 3. 索引同步
        List<String> deleteIndexes = targetTable.getIndexes();
        deleteIndexes.removeAll(originTable.getIndexes());
        for (String index : deleteIndexes) {
            String ddl = String.format("ALTER TABLE `%s` DROP %s;", tableName, index);
            result.add(ddl);
        }
        List<String> addIndexes = originTable.getIndexes();
        addIndexes.removeAll(targetTable.getIndexes());
        for (String index : addIndexes) {
            String ddl = String.format("ALTER TABLE `%s` ADD %s;", tableName, index);
            result.add(ddl);
        }

        // 4. 表的属性比对，如引擎、字符集等


        return result;
    }
}
