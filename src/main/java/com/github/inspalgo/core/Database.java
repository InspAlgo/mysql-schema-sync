package com.github.inspalgo.core;

import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
    private String jdbcUrl;

    private HashMap<String, Table> tableMap = new HashMap<>(128);

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

    public String getJdbcUrl() {
        if (host == null || port == null || dbName == null) {
            throw new IllegalArgumentException("host,port,dbName may be null!");
        }
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true", host, port, dbName);
        }
        return jdbcUrl;
    }

    public void init() {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), username, password)) {
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
                    String ddl = createTableLines[i].trim();
                    if (ddl.charAt(ddl.length() - 1) == ',') {
                        ddl = ddl.substring(0, ddl.length() - 1);
                    }
                    columns.get(i - 1).setDdl(ddl);
                }
                for (int i = columns.size() + 1, end = createTableLines.length - 1; i < end; i++) {
                    String ddl = createTableLines[i].trim();
                    if (ddl.charAt(ddl.length() - 1) == ',') {
                        ddl = ddl.substring(0, ddl.length() - 1);
                    }
                    table.addIndex(ddl);
                }

                table.setAttributes(parseAttributes(createTableLines[createTableLines.length - 1]));

                tableMap.put(tableName, table);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addTables(final List<Table> tables) {
        if (tables == null || tables.size() <= 0) {
            return;
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            Statement statement = connection.createStatement();
            for (Table table : tables) {
                statement.addBatch(table.getCreateTable());
            }
            statement.executeBatch();
            statement.clearBatch();
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteTables(final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            Statement statement = connection.createStatement();
            for (String tableName : tableNames) {
                statement.addBatch("DROP TABLE " + tableName);
            }
            statement.executeBatch();
            statement.clearBatch();
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void syncSchema(final Database sourceDb, final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }

        ThreadPoolExecutor executor = TableThreadPoolExecutor.make(dbName, tableNames.size());
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            CountDownLatch countDownLatch = new CountDownLatch(tableNames.size());
            for (String tableName : tableNames) {
                Connection finalConnection = connection;
                executor.execute(() -> {
                    Table sourceTable = sourceDb.getTableByName(tableName);
                    Table targetTable = getTableByName(tableName);
                    List<String> ddl = SchemaSync.generateTableDdl(sourceTable, targetTable);
                    try {
                        Statement statement = finalConnection.createStatement();
                        for (String s : ddl) {
                            try {
                                statement.addBatch(s);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                        if (!ddl.isEmpty()) {
                            statement.executeBatch();
                            System.out.println(ddl);
                            System.out.printf("SCHEMA [%s] IS SYNC.%n", tableName);
                        }
                        statement.clearBatch();
                        statement.close();
                    } catch (BatchUpdateException e) {
                        int[] updateCounts = e.getUpdateCounts();
                        System.out.println(Arrays.toString(updateCounts));
                        System.out.println(ddl);
                        System.out.println(sourceTable.getCreateTable());
                        System.out.println(targetTable.getCreateTable());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            countDownLatch.await();
            connection.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            executor.shutdownNow();
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void destroyAllAttributes() {
        try {
            dbName = null;
            username = null;
            password = null;
            host = null;
            port = null;
            jdbcUrl = null;
            tableMap.clear();
            tableMap = null;
        } catch (Exception ignore) {

        }
    }

    private static List<String> parseAttributes(String ddl) {
        ArrayList<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder(ddl.length() / 4);
        boolean skip = true;
        for (int i = 0, size = ddl.length(); i < size; i++) {
            char c = ddl.charAt(i);
            if (skip && (c == ')' || c == ' ')) {
                continue;
            }
            skip = false;
            if (c == '=') {
                sb.append(c);
                while (i + 1 < size && ddl.charAt(i + 1) == ' ') {
                    i++;
                }
                for (int k = i + 1; k < size; k++) {
                    c = ddl.charAt(k);
                    if (c == ' ' || c == ';') {
                        i = k;
                        break;
                    }
                    sb.append(c);
                }
                result.add(sb.toString());
                sb.delete(0, sb.length());
                skip = true;
                continue;
            }
            sb.append(c);
        }
        return result;
    }
}
