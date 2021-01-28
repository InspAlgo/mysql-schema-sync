package com.github.inspalgo.logic;

import java.nio.file.Path;

/**
 * @author me
 * @date 2021/1/28 16:40 UTC+08:00
 */
public class TargetMetaData {
    private TargetType type = TargetType.NONE;
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

    public void setType(TargetType type) {
        this.type = type;
    }

    public Object getTarget() {
        return target;
    }

    public void setTarget(Object target) {
        this.target = target;
    }

    public Path getOutputFilePath() {
        return outputFilePath;
    }

    public void setOutputFilePath(Path outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public TargetMetaData(TargetType type, Object target, Path outputFilePath) {
        this.type = type;
        this.target = target;
        this.outputFilePath = outputFilePath;
    }
}
