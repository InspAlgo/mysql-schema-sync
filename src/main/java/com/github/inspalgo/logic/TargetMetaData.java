package com.github.inspalgo.logic;

import com.github.inspalgo.core.ConnectMetaData;

import java.nio.file.Path;

/**
 * @author me
 * @date 2021/1/28 16:40 UTC+08:00
 */
public class TargetMetaData {
    private TargetType type;
    private Object target;
    private Path outputFilePath;

    public enum TargetType {
        /**
         * 无类型
         */
        NONE,
        /**
         * 在线连接
         */
        CONNECT,
        /**
         * SQL 文件
         */
        FILE,
    }

    public TargetType getType() {
        return type;
    }

    public Object getTarget() {
        return target;
    }

    public Path getOutputFilePath() {
        return outputFilePath;
    }

    public TargetMetaData(Object target, Path outputFilePath) {
        this.type = TargetType.NONE;
        this.target = target;
        this.outputFilePath = outputFilePath;
        if (target instanceof ConnectMetaData) {
            this.type = TargetType.CONNECT;
        } else if (target instanceof Path) {
            this.type = TargetType.FILE;
        }
    }
}
