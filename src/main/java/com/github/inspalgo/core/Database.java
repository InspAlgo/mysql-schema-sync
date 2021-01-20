package com.github.inspalgo.core;

import com.github.inspalgo.util.Log;
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
import java.util.concurrent.ConcurrentHashMap;
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

    private HashMap<String, String> addTablesDdlMap;
    private HashMap<String, String> deleteTablesDdlMap;
    private ConcurrentHashMap<String, List<String>> syncSchemaDdlMap;

    public Database setConnectMetaData(ConnectMetaData connectMetaData) {
        dbName = connectMetaData.getDatabase();
        username = connectMetaData.getUsername();
        password = connectMetaData.getPassword();
        host = connectMetaData.getHost();
        port = connectMetaData.getPort();
        return this;
    }

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
                preparedStatementCreateTable.close();
                resultSetCreateTable.close();

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
                preparedStatementColumn.close();
                resultSetColumn.close();

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
                    table.addIndex(parseIndex(createTableLines[i].trim()));
                }

                List<String> attributes = parseAttributes(createTableLines[createTableLines.length - 1]);
                for (int i = 0, size = attributes.size(); i < size; i++) {
                    if (attributes.get(i).startsWith("AUTO_INCREMENT")) {
                        table.setAutoIncrement(attributes.get(i));
                        attributes.remove(i);
                        // 因为调用了 remove 所以 size 要自减，否则越界异常
                        size--;
                    }
                }
                table.setAttributes(attributes);

                tableMap.put(tableName, table);
            }
        } catch (SQLException e) {
            Log.COMMON.error("", e);
        }
    }

    public void generateAddTablesDdlList(final List<Table> tables) {
        if (tables == null || tables.size() <= 0) {
            return;
        }
        if (addTablesDdlMap != null) {
            addTablesDdlMap.clear();
        }
        addTablesDdlMap = new HashMap<>(tables.size());
        for (Table table : tables) {
            String createTable = table.getCreateTable();
            if (table.getAutoIncrement() != null) {
                // 如果存在自增，则需要删除掉表属性中的自增属性，防止带入自增值
                createTable = createTable.replaceAll(table.getAutoIncrement(), "");
            }
            addTablesDdlMap.put(table.getName(), createTable);
        }
    }

    public void addTables() {
        if (addTablesDdlMap == null || addTablesDdlMap.size() == 0) {
            Log.COMMON.error("Not Add Tables DDL.");
            return;
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String tableName : addTablesDdlMap.keySet()) {
                statement.addBatch(addTablesDdlMap.get(tableName));
                Log.COMMON.info("SCHEMA: [{}] IS CREATED.", tableName);
            }
            statement.executeBatch();
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            Log.COMMON.error("", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    Log.COMMON.error("", e);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                Log.COMMON.error("", e);
            }
        }
    }

    public void generateDeleteTablesDdlList(final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }
        if (deleteTablesDdlMap != null) {
            deleteTablesDdlMap.clear();
        }
        deleteTablesDdlMap = new HashMap<>(tableNames.size());
        for (String tableName : tableNames) {
            deleteTablesDdlMap.put(tableName, "DROP TABLE " + tableName);
        }
    }

    public void deleteTables() {
        if (deleteTablesDdlMap == null || deleteTablesDdlMap.size() == 0) {
            Log.COMMON.error("Not Delete Tables DDL.");
            return;
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            for (String tableName : deleteTablesDdlMap.keySet()) {
                statement.addBatch(deleteTablesDdlMap.get(tableName));
                Log.COMMON.info("SCHEMA [{}] IS DELETED.", tableName);
            }
            statement.executeBatch();
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            Log.COMMON.error("", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    Log.COMMON.error("", ex);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                Log.COMMON.error("", e);
            }
        }
    }

    public void generateSyncSchemaDdlList(final Database sourceDb, final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }
        if (syncSchemaDdlMap != null) {
            syncSchemaDdlMap.clear();
        }
        syncSchemaDdlMap = new ConcurrentHashMap<>(tableNames.size());
        ThreadPoolExecutor executor = TableThreadPoolExecutor.make(dbName, tableNames.size());
        CountDownLatch countDownLatch = new CountDownLatch(tableNames.size());
        for (String tableName : tableNames) {
            final ConcurrentHashMap<String, List<String>> ddlMap = syncSchemaDdlMap;
            executor.execute(() -> {
                Table sourceTable = sourceDb.getTableByName(tableName);
                Table targetTable = getTableByName(tableName);
                List<String> ddl = SchemaSync.generateTableDdl(sourceTable, targetTable);
                if (ddl.size() > 0) {
                    ddlMap.put(tableName, ddl);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Log.COMMON.error("", e);
        }
        executor.shutdownNow();
    }

    public void syncSchema() {
        if (deleteTablesDdlMap == null || deleteTablesDdlMap.size() == 0) {
            Log.COMMON.error("Not Sync Schema DDL.");
            return;
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            for (String tableName : syncSchemaDdlMap.keySet()) {
                List<String> ddl = syncSchemaDdlMap.get(tableName);
                try {
                    Statement statement = connection.createStatement();
                    for (String s : ddl) {
                        statement.addBatch(s);
                    }
                    statement.executeBatch();
                    statement.close();
                    Log.COMMON.info("SCHEMA [{}] IS SYNC.", tableName);
                } catch (BatchUpdateException e) {
                    int[] updateCounts = e.getUpdateCounts();
                    Log.COMMON.error(Arrays.toString(updateCounts));
                    Log.COMMON.error(ddl.toString());
                } catch (SQLException e) {
                    Log.COMMON.error("", e);
                }
            }
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    Log.COMMON.error("", ex);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                Log.COMMON.error("", e);
            }
        }
    }

    public void displayPreview() {
        for (String tableName : deleteTablesDdlMap.keySet()) {
            Log.PREVIEW.info(deleteTablesDdlMap.get(tableName));
        }
        for (String tableName : addTablesDdlMap.keySet()) {
            Log.PREVIEW.info(addTablesDdlMap.get(tableName));
        }
        for (String tableName : syncSchemaDdlMap.keySet()) {
            Log.PREVIEW.info(syncSchemaDdlMap.get(tableName).toString());
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

    private void executeCallback(Connection connection, Runnable task) {
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            task.run();
            connection.commit();
        } catch (SQLException e) {
            Log.COMMON.error("", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    Log.COMMON.error("", ex);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                Log.COMMON.error("", e);
            }
        }
    }

    private static String parseIndex(String originDdl) {
        if (originDdl.charAt(originDdl.length() - 1) == ',') {
            originDdl = originDdl.substring(0, originDdl.length() - 1);
        }

        // 因为 BTREE 是默认索引类型，所以比对时先将 USING BTREE 忽略掉
        int btreeIndex = originDdl.lastIndexOf("USING BTREE");
        if (btreeIndex != -1) {
            originDdl = originDdl.substring(0, btreeIndex).trim();
        }

        return originDdl;
    }

    private static List<String> parseAttributes(String ddl) {
        ArrayList<String> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder(ddl.length());
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
