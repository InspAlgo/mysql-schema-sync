package com.github.inspalgo.logic;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.core.Database;
import com.github.inspalgo.core.Table;
import com.github.inspalgo.util.Log;
import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author InspAlgo
 * @date 2021/1/13 14:51 UTC+08:00
 */
public class Dispatcher {
    private boolean preview = false;
    private ConnectMetaData sourceConnectMetaData = null;
    private Path sourcePath = null;
    private List<ConnectMetaData> targetConnectMetaDataList = null;
    private List<Path> outputFilePathList = null;

    public Dispatcher setPreview(boolean preview) {
        this.preview = preview;
        return this;
    }

    public Dispatcher setSourceConnectMetaData(ConnectMetaData sourceConnectMetaData) {
        this.sourceConnectMetaData = sourceConnectMetaData;
        return this;
    }

    public Dispatcher setSourcePath(Path sourcePath) {
        this.sourcePath = sourcePath;
        return this;
    }

    public Dispatcher setTargetConnectMetaDataList(List<ConnectMetaData> targetConnectMetaDataList) {
        this.targetConnectMetaDataList = targetConnectMetaDataList;
        return this;
    }

    public Dispatcher setOutputFilePathList(List<Path> outputFilePathList) {
        this.outputFilePathList = outputFilePathList;
        return this;
    }

    public void onlineSchemaSync()
        throws InterruptedException {
        if (sourceConnectMetaData == null
            || targetConnectMetaDataList == null || targetConnectMetaDataList.isEmpty()) {
            throw new IllegalArgumentException("参数为空");
        }

        Database sourceDb = new Database().setConnectMetaData(sourceConnectMetaData);
        List<Database> targetDbs = new ArrayList<>(targetConnectMetaDataList.size());

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make("Dispatcher",
            targetConnectMetaDataList.size() + 1);

        initDb(executor, sourceDb, targetConnectMetaDataList, targetDbs);

        syncTargets(executor, sourceDb, targetDbs, preview, outputFilePathList);

        executor.shutdownNow();
        sourceDb.destroyAllAttributes();
    }

    public void outlineSchemaSync() throws InterruptedException {
        if (sourcePath == null
            || targetConnectMetaDataList == null || targetConnectMetaDataList.isEmpty()) {
            throw new IllegalArgumentException("参数为空");
        }

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make("Dispatcher",
            targetConnectMetaDataList.size() + 1);
        Database sourceDb = new Database()
            .setDbName(sourcePath.getFileName().toString())
            .setSqlFilePath(sourcePath);
        List<Database> targetDbs = new ArrayList<>(targetConnectMetaDataList.size());

        initDb(executor, sourceDb, targetConnectMetaDataList, targetDbs);

        syncTargets(executor, sourceDb, targetDbs, preview, outputFilePathList);

        executor.shutdownNow();
        sourceDb.destroyAllAttributes();
    }

    private static void initDb(ThreadPoolExecutor executor, Database sourceDb,
                               List<ConnectMetaData> targets, List<Database> targetDbs) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(targets.size() + 1);

        executor.execute(() -> {
            try {
                sourceDb.init();
            } catch (Exception e) {
                Log.COMMON.error("Source Database Init Exception", e);
            } finally {
                countDownLatch.countDown();
            }
        });
        for (ConnectMetaData connectMetaData : targets) {
            Database targetDb = new Database().setConnectMetaData(connectMetaData);
            targetDbs.add(targetDb);
            executor.execute(() -> {
                try {
                    targetDb.init();
                } catch (Exception e) {
                    Log.COMMON.error("Target Database [{}] Init Exception", targetDb.getDbName());
                    Log.COMMON.error("", e);
                }
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            throw e;
        }
    }

    private static void syncTargets(ThreadPoolExecutor executor, Database sourceDb,
                                    List<Database> targetDbs, boolean preview, List<Path> outputFilePathList) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(targetDbs.size());

        for (int i = 0, size = targetDbs.size(); i < size; i++) {
            final Database targetDb = targetDbs.get(i);
            if (outputFilePathList != null) {
                targetDb.setOutputDdlFilepath(outputFilePathList.get(i));
            }
            executor.execute(() -> {
                // 要删除的旧表
                ArrayList<String> deleteTableNames = targetDb.getAllTableNames();
                deleteTableNames.removeAll(sourceDb.getAllTableNames());
                targetDb.generateDeleteTablesDdlList(deleteTableNames);

                // 要创建的新表
                ArrayList<String> createTableNames = sourceDb.getAllTableNames();
                createTableNames.removeAll(targetDb.getAllTableNames());
                ArrayList<Table> createdTables = new ArrayList<>(createTableNames.size());
                createTableNames.forEach(tableName -> createdTables.add(sourceDb.getTableByName(tableName)));
                targetDb.generateAddTablesDdlList(createdTables);

                // 可能要修改的表
                ArrayList<String> modifyTableNames = sourceDb.getAllTableNames();
                modifyTableNames.retainAll(targetDb.getAllTableNames());
                targetDb.generateSyncSchemaDdlList(sourceDb, modifyTableNames);

                if (preview) {
                    Log.PREVIEW.info("=== `{}` DDL Preview Start ===", targetDb.getDbName());
                    targetDb.displayPreview();
                    Log.PREVIEW.info("=== `{}` DDL Preview End ===", targetDb.getDbName());
                } else {
                    targetDb.deleteTables();
                    targetDb.addTables();
                    targetDb.syncSchema();
                }

                targetDb.outputDdlFile();

                targetDb.destroyAllAttributes();
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            throw e;
        }
    }
}
