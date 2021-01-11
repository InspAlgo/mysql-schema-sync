package com.github.inspalgo.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author me
 * @date 2021/1/11 11:25 UTC+08:00
 */
public class TableThreadPoolExecutor {
    public static ThreadPoolExecutor make(String name) {
        return new ThreadPoolExecutor(getCorePoolSize(), getMaxPoolSize(), 2L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(20), new TableThreadFactory(name),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public static ThreadPoolExecutor make(String name, int blockingQueueCapacity) {
        return new ThreadPoolExecutor(getCorePoolSize(), getMaxPoolSize(), 2L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(blockingQueueCapacity), new TableThreadFactory(name),
            new ThreadPoolExecutor.AbortPolicy());
    }

    private static int getCorePoolSize() {
        int corePoolSize = Runtime.getRuntime().availableProcessors() / 4;
        return corePoolSize <= 0 ? 1 : corePoolSize;
    }

    private static int getMaxPoolSize() {
        int maxPoolSize = Runtime.getRuntime().availableProcessors() / 2;
        return maxPoolSize <= 0 ? 1 : maxPoolSize;
    }
}
