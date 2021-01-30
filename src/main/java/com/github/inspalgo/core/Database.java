package com.github.inspalgo.core;

import com.github.inspalgo.util.Log;
import com.github.inspalgo.util.TableThreadPoolExecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    private Path sqlFilePath;
    private Path outputDdlFilepath;

    private HashMap<String, Table> tableMap = new HashMap<>(128);

    private HashMap<String, String> addTablesDdlMap = new HashMap<>(2);
    private HashMap<String, String> deleteTablesDdlMap = new HashMap<>(2);
    private ConcurrentHashMap<String, List<String>> syncSchemaDdlMap = new ConcurrentHashMap<>(2);

    public Database setConnectMetaData(ConnectMetaData connectMetaData) {
        dbName = connectMetaData.getDatabase();
        username = connectMetaData.getUsername();
        password = connectMetaData.getPassword();
        host = connectMetaData.getHost();
        port = connectMetaData.getPort();
        return this;
    }

    public boolean checkConnectMetaData() {
        String[] checks = new String[]{
            dbName, username, password, host, port
        };

        for (String check : checks) {
            if (check == null || check.isEmpty()) {
                return false;
            }
        }

        return true;
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
            throw new IllegalArgumentException(String.format(
                "host[%s], port[%s],dbName[%s] may be null!", host, port, dbName));
        }
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true", host, port, dbName);
        }
        return jdbcUrl;
    }

    public Path getSqlFilePath() {
        return sqlFilePath;
    }

    public Database setSqlFilePath(Path sqlFilePath) {
        if (sqlFilePath == null) {
            throw new IllegalArgumentException("The sqlFilePath is null!");
        }
        this.sqlFilePath = sqlFilePath;
        dbName = sqlFilePath.getFileName().toString();
        return this;
    }

    public Database setOutputDdlFilepath(Path outputDdlFilepath) {
        this.outputDdlFilepath = outputDdlFilepath;
        return this;
    }

    public void init() {
        if (checkConnectMetaData()) {
            initByOnline();
        } else if (sqlFilePath != null) {
            initBySqlFile();
        } else {
            throw new RuntimeException("数据库初始化异常");
        }
    }

    public void initByOnline() {
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
                if (rowFormat != null && !rowFormat.isEmpty()) {
                    table.setRowFormat(rowFormat);
                }
                String createTable = "";

                String queryCreateTable = "SHOW CREATE TABLE " + tableName;
                PreparedStatement preparedStatementCreateTable = connection.prepareStatement(queryCreateTable);
                ResultSet resultSetCreateTable = preparedStatementCreateTable.executeQuery();
                while (resultSetCreateTable.next()) {
                    createTable = resultSetCreateTable.getString(2);
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

                String[] createTableLines = createTable.split("\n");
                List<Column> columns = table.getColumns();
                for (int i = 1, columnSize = columns.size(); i <= columnSize; i++) {
                    columns.get(i - 1).setDdl(removeLastComma(createTableLines[i].trim()));
                }
                for (int i = columns.size() + 1, end = createTableLines.length - 1; i < end; i++) {
                    String index = parseIndex(createTableLines[i].trim());
                    if (index.startsWith("PRIMARY KEY")) {
                        table.setPrimaryKey(index);
                        continue;
                    }
                    table.addIndex(index);
                }

                parseAttributes(table, createTableLines[createTableLines.length - 1]);

                if (table.selfCheck()) {
                    tableMap.put(tableName, table);
                } else {
                    Log.COMMON.error("`{}`.`{}` 数据异常", dbName, tableName);
                }
            }
        } catch (SQLException e) {
            Log.COMMON.error("", e);
        }
    }

    public void initBySqlFile() {
        try (BufferedReader reader = Files.newBufferedReader(sqlFilePath, StandardCharsets.UTF_8)) {
            String line;
            Table table = null;
            String tableName = null;
            int columnOrdinalPosition = 1;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (table == null && line.startsWith("CREATE TABLE")) {
                    tableName = line.substring(
                        line.contains("`.`") ? line.indexOf("`.`") + 3 : line.indexOf('`') + 1,
                        line.lastIndexOf('`')
                    );
                    table = new Table();
                    table.setName(tableName);
                } else if (table != null && line.startsWith("`")) {
                    Column column = new Column()
                        .setColumnName(line.substring(1, line.indexOf('`', 1)))
                        .setOrdinalPosition(columnOrdinalPosition++)
                        .setDdl(removeLastComma(line));
                    table.addColumn(column);
                } else if (table != null && line.startsWith("PRIMARY KEY")) {
                    table.setPrimaryKey(parseIndex(line));
                } else if (table != null && line.startsWith(")")) {
                    parseAttributes(table, line);

                    if (table.selfCheck()) {
                        tableMap.put(tableName, table);
                    } else {
                        Log.COMMON.error("`{}`.`{}` 数据异常", dbName, tableName);
                    }
                    table = null;
                    tableName = null;
                    columnOrdinalPosition = 1;
                } else if (table != null && (line.contains("KEY") || line.contains("INDEX"))) {
                    table.addIndex(parseIndex(line));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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
        tables.forEach(table -> addTablesDdlMap.put(table.getName(), table.getCreateTable()));
    }

    public void generateDeleteTablesDdlList(final List<String> tableNames) {
        if (tableNames == null || tableNames.size() <= 0) {
            return;
        }
        if (deleteTablesDdlMap != null) {
            deleteTablesDdlMap.clear();
        }
        deleteTablesDdlMap = new HashMap<>(tableNames.size());
        tableNames.forEach(tblName -> deleteTablesDdlMap.put(tblName, "DROP TABLE `" + tblName + "`"));
    }

    public void deleteAndAddTables() {
        executeTask(connection -> {
            Statement statement = connection.createStatement();
            List<String> ddlList = new ArrayList<>(deleteTablesDdlMap.size() + addTablesDdlMap.size());
            for (String tableName : deleteTablesDdlMap.keySet()) {
                String ddl = deleteTablesDdlMap.get(tableName);
                statement.addBatch(ddl);
                ddlList.add(ddl);
            }
            for (String tableName : addTablesDdlMap.keySet()) {
                String ddl = addTablesDdlMap.get(tableName);
                statement.addBatch(ddl);
                ddlList.add(ddl);
            }
            try {
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                handleBatchUpdateException(connection, e, ddlList);
            }
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
            return;
        }

        executeTask(connection -> {
            Statement statement = connection.createStatement();
            for (String tableName : syncSchemaDdlMap.keySet()) {
                List<String> ddlList = syncSchemaDdlMap.get(tableName);
                try {
                    for (String s : ddlList) {
                        statement.addBatch(s);
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                } catch (BatchUpdateException e) {
                    handleBatchUpdateException(connection, e, ddlList);
                }
            }
            statement.close();
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
     * 输出 DDL 文件
     */
    public void outputDdlFile() {
        if (outputDdlFilepath == null) {
            return;
        }
        try (BufferedWriter writer = Files.newBufferedWriter(outputDdlFilepath,
            StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (String ddl : deleteTablesDdlMap.values()) {
                writer.write(ddl);
                writer.write(";\n");
            }
            for (String ddl : addTablesDdlMap.values()) {
                writer.write(ddl);
                writer.write(";\n");
            }
            for (List<String> ddlList : syncSchemaDdlMap.values()) {
                for (String ddl : ddlList) {
                    writer.write(ddl);
                    writer.write(";\n");
                }
            }
            writer.flush();
        } catch (IOException e) {
            Log.COMMON.error("`{}` 生成DDL输出文件失败.\n{}", dbName, e);
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
        if (!checkConnectMetaData()) {
            return;
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(getJdbcUrl(), username, password);
            connection.setAutoCommit(false);
            connection.prepareStatement("SET FOREIGN_KEY_CHECKS = 0").execute();
            task.run(connection);
            connection.prepareStatement("SET FOREIGN_KEY_CHECKS = 1").execute();
            connection.commit();
        } catch (SQLException e) {
            Log.COMMON.error("Execute Task Exception", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    Log.COMMON.error("Rollback Exception", ex);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                Log.COMMON.error("Connection Close Exception", e);
            }
        }
    }

    /**
     * 处理 Statement 批量执行异常
     *
     * @param connection JDBC 连接
     * @param e          Statement 批量执行异常
     * @param ddlList    Statement 批量执行的 DDL 语句
     * @throws SQLException JDBC 执行异常
     */
    private void handleBatchUpdateException(Connection connection, BatchUpdateException e, List<String> ddlList)
        throws SQLException {
        int[] updateCounts = e.getUpdateCounts();
        for (int i = 0; i < updateCounts.length; i++) {
            if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                Log.COMMON.error("`{}` Execute Failed: {}.", dbName, ddlList.get(i));
            } else {
                Log.COMMON.error("`{}` Execute Succeed: {}.", dbName, ddlList.get(i));
            }
        }
        connection.rollback();
    }

    private static String removeLastComma(String ddl) {
        if (ddl.charAt(ddl.length() - 1) == ',') {
            ddl = ddl.substring(0, ddl.length() - 1);
        }
        return ddl;
    }

    private static StringBuilder removeLastComma(StringBuilder ddl) {
        if (ddl.charAt(ddl.length() - 1) == ',') {
            ddl.deleteCharAt(ddl.length() - 1);
        }
        return ddl;
    }

    /**
     * 主键、索引解析
     *
     * @param originDdl 主键、索引，CREATE TABLE 的字段属性到最后一行的中间部分
     * @return 主键、索引
     */
    private static String parseIndex(String originDdl) {
        StringBuilder sb = removeLastComma(new StringBuilder(originDdl));

        // 因为 BTREE 是默认索引类型，所以不含 USING 的索引默认为使用 BTREE
        if (sb.indexOf("USING") == -1) {
            sb.append(" USING BTREE");
        }

        return sb.toString();
    }

    /**
     * 表属性解析
     *
     * @param ddl 表属性，CREATE TABLE 的最后一行
     * @return 表属性列表
     */
    private static void parseAttributes(Table table, String ddl) {
        StringBuilder key = new StringBuilder(ddl.length() / 2);
        StringBuilder value = new StringBuilder(ddl.length() / 2);
        boolean skip = true;
        for (int i = 0, size = ddl.length(); i < size; i++) {
            char c = ddl.charAt(i);
            if (skip && (c == ')' || c == ' ')) {
                continue;
            }
            skip = false;
            if (c == '=') {
                while (key.charAt(key.length() - 1) == ' ') {
                    key.deleteCharAt(key.length() - 1);
                }

                while (i + 1 < size && ddl.charAt(i + 1) == ' ') {
                    i++;
                }
                for (int k = i + 1; k < size; k++) {
                    c = ddl.charAt(k);
                    if (c == ' ' || c == ';') {
                        i = k;
                        break;
                    }
                    value.append(c);
                }

                handleAttribute(table, key, value);

                key.delete(0, key.length());
                value.delete(0, value.length());
                skip = true;
                continue;
            }
            key.append(c);
        }
    }

    private static void handleAttribute(Table table, StringBuilder key, StringBuilder value) {
        if (key.indexOf("ENGINE") != -1) {
            table.setEngine(value.toString());
        } else if (key.indexOf("AUTO_INCREMENT") != -1) {
            table.setAutoIncrement(value.toString());
        } else if (key.indexOf("ROW_FORMAT") != -1) {
            table.setRowFormat(value.toString());
        } else if (key.indexOf("DEFAULT CHARSET") != -1 || key.indexOf("CHARACTER SET") != -1) {
            table.setCharset(value.toString());
        } else {
            table.addAttribute(key.append("=").append(value).toString());
        }
    }
}
