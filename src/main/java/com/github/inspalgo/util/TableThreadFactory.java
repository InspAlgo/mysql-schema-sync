package com.github.inspalgo.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author InspAlgo
 * @date 2021/1/11 11:18 UTC+08:00
 */
public class TableThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger nextId = new AtomicInteger(1);

    public TableThreadFactory(String whatFeatureOfGroup) {
        namePrefix = whatFeatureOfGroup + "-Worker-";
    }

    @Override
    public Thread newThread(Runnable task) {
        String name = namePrefix + nextId.getAndIncrement();
        return new Thread(task, name);
    }
}
