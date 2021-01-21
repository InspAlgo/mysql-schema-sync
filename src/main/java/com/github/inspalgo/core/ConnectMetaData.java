package com.github.inspalgo.core;

/**
 * @author InspAlgo
 * @date 2021/1/13 16:47 UTC+08:00
 */
public class ConnectMetaData {
    private Type type = Type.NONE;
    private String username;
    private String password;
    private String host;
    private String port;
    private String database;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public String toString() {
        return "ConnectMetaData{" +
            "type=" + type +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", host='" + host + '\'' +
            ", port='" + port + '\'' +
            ", database='" + database + '\'' +
            '}';
    }

    /**
     * 数据库类型
     */
    public enum Type {
        /**
         * 无类型，用于填充默认值
         */
        NONE,
        /**
         * MySQL 数据库
         */
        MYSQL,
    }
}
