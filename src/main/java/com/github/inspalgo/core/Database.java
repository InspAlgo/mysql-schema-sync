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
            String queryTables = "SELECT TABLE_NAME,ROW_FORMAT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?";
            PreparedStatement preparedStatementTable = connection.prepareStatement(queryTables);
            preparedStatementTable.setString(1, dbName);
            ResultSet resultSetTable = preparedStatementTable.executeQuery();

            while (resultSetTable.next()) {
                Table table = new Table();

                String tableName = resultSetTable.getString("TABLE_NAME");
                table.setName(tableName);
                String rowFormat = resultSetTable.getString("ROW_FORMAT");

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
                    String index = parseIndex(createTableLines[i].trim());
                    if (index.startsWith("PRIMARY KEY")) {
                        table.setPrimaryKey(index);
                        continue;
                    }
                    table.addIndex(index);
                }

                List<String> attributes = parseAttributes(createTableLines[createTableLines.length - 1]);
                for (String attribute : attributes) {
                    if (attribute.startsWith("AUTO_INCREMENT")) {
                        table.setAutoIncrement(attribute);
                    } else {
                        table.addAttribute(attribute);
                    }
                }
                if (rowFormat != null && !rowFormat.isEmpty()) {
                    table.addAttribute(String.format("ROW_FORMAT=%s", rowFormat));
                }

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
        tables.forEach(table -> addTablesDdlMap.put(table.getName(), table.getNewCreateTable()));
    }

    public void addTables() {
        if (addTablesDdlMap == null || addTablesDdlMap.size() == 0) {
            Log.COMMON.error("`{}` Not Add Tables DDL.", dbName);
            return;
        }

        executeTask(connection -> {
            Statement statement = connection.createStatement();
            for (String tableName : addTablesDdlMap.keySet()) {
                statement.addBatch(addTablesDdlMap.get(tableName));
                Log.COMMON.info("TABLE `{}`.`{}` IS CREATED.", dbName, tableName);
            }
            statement.executeBatch();
            statement.close();
        });
    }

    public void generateDeleteTablesDdlList(final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }
        if (deleteTablesDdlMap != null) {
            deleteTablesDdlMap.clear();
        }
        deleteTablesDdlMap = new HashMap<>(tableNames.size());
        tableNames.forEach(tblName -> deleteTablesDdlMap.put(tblName, String.format("DROP TABLE `%s`", tblName)));
    }

    public void deleteTables() {
        if (deleteTablesDdlMap == null || deleteTablesDdlMap.size() == 0) {
            Log.COMMON.error("`{}` Not Delete Tables DDL.", dbName);
            return;
        }

        executeTask(connection -> {
            Statement statement = connection.createStatement();
            for (String tableName : deleteTablesDdlMap.keySet()) {
                statement.addBatch(deleteTablesDdlMap.get(tableName));
                Log.COMMON.info("TABLE `{}`.`{}` IS DELETED.", dbName, tableName);
            }
            statement.executeBatch();
            statement.close();
        });
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
        if (syncSchemaDdlMap == null || syncSchemaDdlMap.size() == 0) {
            Log.COMMON.error("`{}` Not Sync Schema DDL.", dbName);
            return;
        }

        executeTask(connection -> {
            for (String tableName : syncSchemaDdlMap.keySet()) {
                List<String> ddl = syncSchemaDdlMap.get(tableName);
                try {
                    Statement statement = connection.createStatement();
                    for (String s : ddl) {
                        statement.addBatch(s);
                    }
                    statement.executeBatch();
                    statement.close();
                    Log.COMMON.info("TABLE `{}`.`{}` IS SYNCHRONIZED.", dbName, tableName);
                } catch (BatchUpdateException e) {
                    String table = String.format("`%s`.`%s`", dbName, tableName);
                    int[] updateCounts = e.getUpdateCounts();
                    for (int i = 0; i < updateCounts.length; i++) {
                        if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                            Log.COMMON.error("{} Execute Failed: {}.", table, ddl.get(i));
                        } else {
                            Log.COMMON.error("{} Execute Succeed: {}.", table, ddl.get(i));
                        }
                    }
                }
            }
        });
    }

    /**
     * 显示预览
     */
    public void displayPreview() {
        if (deleteTablesDdlMap != null) {
            deleteTablesDdlMap.forEach((k, v) -> Log.PREVIEW.info(v));
        }
        if (addTablesDdlMap != null) {
            addTablesDdlMap.forEach((k, v) -> Log.PREVIEW.info(v));
        }
        if (syncSchemaDdlMap != null) {
            syncSchemaDdlMap.forEach((k, v) -> Log.PREVIEW.info(v.toString()));
        }
    }

    /**
     * 销毁属性
     */
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

    @FunctionalInterface
    private interface Task {
        /**
         * 数据库任务执行
         *
         * @param connection JDBC 的 {@link Connection} 实例
         * @throws SQLException JDBC 执行时抛出的 SQL 异常
         */
        void run(Connection connection) throws SQLException;
    }

    /**
     * 数据库任务执行，将事物提交与回滚等公共操作提取出来，只需传入要执行的 Task 即可
     *
     * @param task 要执行的数据库任务
     */
    private void executeTask(Task task) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            task.run(connection);
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

    /**
     * 主键、索引解析
     *
     * @param originDdl 主键、索引，CREATE TABLE 的字段属性到最后一行的中间部分
     * @return 主键、索引
     */
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

    /**
     * 表属性解析
     *
     * @param ddl 表属性，CREATE TABLE 的最后一行
     * @return 表属性列表
     */
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
