package com.github.inspalgo;

import com.github.inspalgo.core.Database;
import com.github.inspalgo.core.Table;
import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author InspAlgo
 * @date 2021/1/7 20:19 UTC+08:00
 */
public class Main {

    public static void main(String[] args) {
        String sourceDbName = args[0];
        if (sourceDbName == null || sourceDbName.isEmpty()) {
            System.exit(-1);
        }
        String targetDbName = args[1];
        if (targetDbName == null || targetDbName.isEmpty()) {
            System.exit(-1);
        }

        Database sourceDb = new Database();
        sourceDb.setDbName(sourceDbName).setHost("127.0.0.1").setPort("3306").setUsername("root").setPassword("root");
        Database targetDb = new Database();
        targetDb.setDbName(targetDbName).setHost("127.0.0.1").setPort("3306").setUsername("root").setPassword("root");

        CountDownLatch countDownLatch = new CountDownLatch(2);

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make("AccessTables");
        executor.execute(() -> {
            sourceDb.init();
            countDownLatch.countDown();
        });
        executor.execute(() -> {
            targetDb.init();
            countDownLatch.countDown();
        });

        try {
            countDownLatch.await();
            executor.shutdownNow();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 要创建的新表
        ArrayList<String> createTableNames = sourceDb.getAllTableNames();
        createTableNames.removeAll(targetDb.getAllTableNames());
        // 可能要修改的表
        ArrayList<String> modifyTableNames = sourceDb.getAllTableNames();
        modifyTableNames.retainAll(targetDb.getAllTableNames());
        // 要删除的旧表
        ArrayList<String> deleteTableNames = targetDb.getAllTableNames();
        deleteTableNames.removeAll(sourceDb.getAllTableNames());

        System.out.println("==== Run Create Tables ====");
        ArrayList<Table> createdTables = new ArrayList<>(createTableNames.size());
        createTableNames.forEach(tableName -> createdTables.add(sourceDb.getTableByName(tableName)));
        targetDb.addTables(createdTables);
        System.out.println();

        System.out.println("==== Run Delete Tables ====");
        targetDb.deleteTables(deleteTableNames);
        System.out.println();

        System.out.println("==== Run Sync Schema ====");
        targetDb.syncSchema(sourceDb, modifyTableNames);
        System.out.println();

        sourceDb.destroyAllAttributes();
        targetDb.destroyAllAttributes();
    }
}
