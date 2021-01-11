package com.github.inspalgo.core;

import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author InspAlgo
 * @date 2021/1/11 15:44 UTC+08:00
 */
public class Database {
    private String dbName;
    private String username;
    private String password;
    private String host;
    private String port;

    private HashMap<String, Table> tableMap = new HashMap<>(128);

    private ThreadPoolExecutor executor = TableThreadPoolExecutor.make("Database");

    public String getDbName() {
        return dbName;
    }

    public Database setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public Database setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Database setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getHost() {
        return host;
    }

    public Database setHost(String host) {
        this.host = host;
        return this;
    }

    public String getPort() {
        return port;
    }

    public Database setPort(String port) {
        this.port = port;
        return this;
    }

    public HashMap<String, Table> getTableMap() {
        return tableMap;
    }

    public void setTableMap(HashMap<String, Table> tableMap) {
        this.tableMap = tableMap;
    }

    public ArrayList<String> getAllTableNames() {
        ArrayList<String> result = new ArrayList<>(tableMap.size());
        tableMap.forEach((k, v) -> result.add(k));
        return result;
    }

    public Table getTableByName(String tableName) {
        return tableMap.getOrDefault(tableName, null);
    }

    public void accessTables() {
        try (Connection connection = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%s/%s?useUnicode=true", host, port, dbName), username, password)) {
            String queryTables = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?";
            PreparedStatement preparedStatementTable = connection.prepareStatement(queryTables);
            preparedStatementTable.setString(1, dbName);
            ResultSet resultSetTable = preparedStatementTable.executeQuery();

            while (resultSetTable.next()) {
                Table table = new Table();

                String tableName = resultSetTable.getString("TABLE_NAME");
                table.setName(tableName);

                String queryCreateTable = "SHOW CREATE TABLE " + tableName;
                PreparedStatement preparedStatementCreateTable = connection.prepareStatement(queryCreateTable);
                ResultSet resultSetCreateTable = preparedStatementCreateTable.executeQuery();
                while (resultSetCreateTable.next()) {
                    table.setCreateTable(resultSetCreateTable.getString(2));
                }

                String queryColumns = "SELECT COLUMN_NAME,ORDINAL_POSITION " +
                    "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
                PreparedStatement preparedStatementColumn = connection.prepareStatement(queryColumns);
                preparedStatementColumn.setString(1, dbName);
                preparedStatementColumn.setString(2, tableName);
                ResultSet resultSetColumn = preparedStatementColumn.executeQuery();

                while (resultSetColumn.next()) {
                    Column column = new Column();
                    column.setColumnName(resultSetColumn.getString("COLUMN_NAME"))
                          .setOrdinalPosition(resultSetColumn.getString("ORDINAL_POSITION"));
                    table.addColumn(column);
                }

                String[] createTableLines = table.getCreateTable().split("\n");
                List<Column> columns = table.getColumns();
                for (int i = 1, columnSize = columns.size(); i <= columnSize; i++) {
                    columns.get(i - 1).setDdl(createTableLines[i].trim().replace(",", ""));
                }
                for (int i = columns.size() + 1, end = createTableLines.length - 1; i < end; i++) {
                    table.addIndex(createTableLines[i].trim().replace(",", ""));
                }

                tableMap.put(tableName, table);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addTables(List<Table> tables) {
        if (tables == null || tables.size() <= 0) {
            return;
        }

        try (Connection connection = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%s/%s?useUnicode=true", host, port, dbName), username, password)) {
            CountDownLatch countDownLatch = new CountDownLatch(tables.size());
            for (Table table : tables) {
                executor.execute(() -> {
                    try {
                        String createTable = table.getCreateTable();
                        PreparedStatement ps = connection.prepareStatement(createTable);
                        ps.execute();
                        System.out.printf("TABLE [%s] IS CREATED.%n", table.getName());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            countDownLatch.await();
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void deleteTables(List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }

        try (Connection connection = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%s/%s?useUnicode=true", host, port, dbName), username, password)) {
            CountDownLatch countDownLatch = new CountDownLatch(tableNames.size());
            for (String tableName : tableNames) {
                executor.execute(() -> {
                    try {
                        String deleteTable = "DROP TABLE " + tableName;
                        PreparedStatement ps = connection.prepareStatement(deleteTable);
                        ps.execute();
                        System.out.printf("TABLE [%s] IS DELETED.%n", tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            countDownLatch.await();
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void destroyAllAttributes() {
        try {
            dbName = null;
            username = null;
            password = null;
            host = null;
            port = null;
            tableMap.clear();
            tableMap = null;
            executor.shutdownNow();
        } catch (Exception ignore) {

        }
    }
}
