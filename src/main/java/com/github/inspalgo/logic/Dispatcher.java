package com.github.inspalgo.logic;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.core.Database;
import com.github.inspalgo.core.Table;
import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author InspAlgo
 * @date 2021/1/13 14:51 UTC+08:00
 */
public class Dispatcher {
    public static void onlineSchemaSync(ConnectMetaData source, List<ConnectMetaData> targets) {
        if (source == null || targets == null || targets.isEmpty()) {
            return;
        }

        Database sourceDb = new Database().setConnectMetaData(source);
        List<Database> targetDbs = new ArrayList<>(targets.size());

        CountDownLatch countDownLatch = new CountDownLatch(targets.size() + 1);

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make("AccessTables", targets.size() + 1);
        executor.execute(() -> {
            sourceDb.init();
            countDownLatch.countDown();
        });
        for (ConnectMetaData connectMetaData : targets) {
            Database targetDb = new Database().setConnectMetaData(connectMetaData);
            targetDbs.add(targetDb);
            executor.execute(() -> {
                targetDb.init();
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }

        CountDownLatch countDownLatchSync = new CountDownLatch(targetDbs.size());
        for (Database targetDb : targetDbs) {
            executor.execute(() -> {
                // 要创建的新表
                ArrayList<String> createTableNames = sourceDb.getAllTableNames();
                createTableNames.removeAll(targetDb.getAllTableNames());
                // 可能要修改的表
                ArrayList<String> modifyTableNames = sourceDb.getAllTableNames();
                modifyTableNames.retainAll(targetDb.getAllTableNames());
                // 要删除的旧表
                ArrayList<String> deleteTableNames = targetDb.getAllTableNames();
                deleteTableNames.removeAll(sourceDb.getAllTableNames());

                System.out.println("==== Run Delete Tables ====");
                targetDb.deleteTables(deleteTableNames);
                System.out.println();

                System.out.println("==== Run Create Tables ====");
                ArrayList<Table> createdTables = new ArrayList<>(createTableNames.size());
                createTableNames.forEach(tableName -> createdTables.add(sourceDb.getTableByName(tableName)));
                targetDb.addTables(createdTables);
                System.out.println();

                System.out.println("==== Run Sync Schema ====");
                targetDb.syncSchema(sourceDb, modifyTableNames);
                System.out.println();

                targetDb.destroyAllAttributes();
                countDownLatchSync.countDown();
            });
        }

        try {
            countDownLatchSync.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }
        sourceDb.destroyAllAttributes();
    }
}
