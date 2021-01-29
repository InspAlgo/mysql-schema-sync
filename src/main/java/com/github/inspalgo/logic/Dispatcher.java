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
    private Object source = null;
    private List<TargetMetaData> targetList = null;

    public Dispatcher setPreview(boolean preview) {
        this.preview = preview;
        return this;
    }

    public Dispatcher setSource(Object source) {
        this.source = source;
        return this;
    }

    public Dispatcher setTargetList(List<TargetMetaData> targetList) {
        this.targetList = targetList;
        return this;
    }

    public void schemaSync() throws InterruptedException {
        if (source == null || targetList == null || targetList.isEmpty()) {
            throw new IllegalArgumentException("参数为空");
        }

        Database sourceDb = new Database();
        List<Database> targetDbs = new ArrayList<>(targetList.size());

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make("Dispatcher", targetList.size() + 1);
        try {
            CountDownLatch countDownLatch = new CountDownLatch(targetList.size() + 1);
            initSourceDb(executor, sourceDb, countDownLatch);
            initTargetDbList(executor, targetDbs, targetList, countDownLatch);
            countDownLatch.await();
            syncTargets(executor, sourceDb, targetDbs);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            throw e;
        } finally {
            executor.shutdownNow();
            sourceDb.destroyAllAttributes();
        }
    }

    private void initSourceDb(ThreadPoolExecutor executor, Database sourceDb, CountDownLatch countDownLatch) {
        if (source instanceof ConnectMetaData) {
            sourceDb.setConnectMetaData((ConnectMetaData) source);
        } else if (source instanceof Path) {
            sourceDb.setSqlFilePath((Path) source);
        }
        executor.execute(() -> {
            try {
                sourceDb.init();
            } catch (Exception e) {
                Log.COMMON.error("Source Database Init Exception", e);
            } finally {
                countDownLatch.countDown();
            }
        });
    }

    private void initTargetDbList(ThreadPoolExecutor executor, List<Database> targetDbs,
                                  List<TargetMetaData> targets, CountDownLatch countDownLatch) {
        for (TargetMetaData targetMetaData : targets) {
            Database targetDb = new Database();

            switch (targetMetaData.getType()) {
                case CONNECT:
                    targetDb.setConnectMetaData((ConnectMetaData) targetMetaData.getTarget());
                    break;
                case FILE:
                    targetDb.setSqlFilePath((Path) targetMetaData.getTarget());
                    break;
                default:
                    Log.COMMON.error("目标类型错误 {}，该目标同步跳过", targetMetaData.getTarget());
                    continue;
            }
            targetDb.setOutputDdlFilepath(targetMetaData.getOutputFilePath());

            targetDbs.add(targetDb);

            executor.execute(() -> {
                try {
                    targetDb.init();
                } catch (Exception e) {
                    Log.COMMON.error("Target Database [{}] Init Exception", targetDb.getDbName());
                    Log.COMMON.error("", e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
    }

    private void syncTargets(ThreadPoolExecutor executor, Database sourceDb, List<Database> targetDbs)
        throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(targetDbs.size());

        for (final Database targetDb : targetDbs) {
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
                    targetDb.deleteAndAddTables();
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
