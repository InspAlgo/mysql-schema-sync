package com.github.inspalgo.command;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.logic.Dispatcher;
import com.github.inspalgo.util.Log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * @author InspAlgo
 * @date 2021/1/13 15:03 UTC+08:00
 */
@Command(name = "MySQL Schema Sync", mixinStandardHelpOptions = true, version = "MySQL Schema Sync v0.3")
public class Arguments implements Runnable {
    @Option(names = {"-s", "--source"}, required = true, description = "指定源：1.在线方式 -s mysql#username:password@host:port/database_name, 2.SQL文件方式  -s sql_filepath")
    private String source;

    @Option(names = {"-t", "--target"}, required = true, description = "指定目标：目前只有在线方式 -t mysql#username:password@host:port/database_name")
    private List<String> targets;

    @Option(names = {"-p", "--preview"}, description = "Executing Process Preview")
    private boolean preview = false;

    @Override
    public void run() {
        ConnectMetaData sourceConnectMetaData = null;
        Path sourceFilepath = null;
        List<ConnectMetaData> targetConnectMetaDataList = null;

        SourceType sourceType = SourceType.NONE;
        if (source != null) {
            sourceType = SourceType.ONLINE;
            sourceConnectMetaData = parseUri(source);
            if (sourceConnectMetaData == null) {
                sourceType = SourceType.SQL_FILE;
                sourceFilepath = parseFilepath(source);
            }
            if (sourceConnectMetaData == null && sourceFilepath == null) {
                sourceType = SourceType.NONE;
                Log.COMMON.error("源数据库资源标识格式错误: [{}]", source);
                System.exit(-1);
            }
        }

        if (targets != null && targets.size() > 0) {
            targetConnectMetaDataList = new ArrayList<>(targets.size());

            for (String t : targets) {
                ConnectMetaData targetConnectMetaData = parseUri(t);
                if (targetConnectMetaData != null) {
                    targetConnectMetaDataList.add(targetConnectMetaData);
                } else {
                    Log.COMMON.error("目标数据库资源标识格式错误: [{}]", t);
                    System.exit(-1);
                }
            }
        }

        try {
            switch (sourceType) {
                case ONLINE:
                    Dispatcher.onlineSchemaSync(sourceConnectMetaData, targetConnectMetaDataList, preview);
                    break;
                case SQL_FILE:
                    Dispatcher.outlineSchemaSync(sourceFilepath, targetConnectMetaDataList, preview);
                    break;
                default:
                    Log.COMMON.error("源类型未知");
                    break;
            }
        } catch (Exception e) {
            Log.COMMON.error("", e);
            System.exit(-1);
        }
    }

    /**
     * 源类型
     */
    private enum SourceType {
        /**
         * 无类型
         */
        NONE,
        /**
         * 在线式
         */
        ONLINE,
        /**
         * SQL 文件
         */
        SQL_FILE,
    }

    /**
     * 解析 mysql#username:password@host:port/database_name 形式的标识
     *
     * @param uri 在线密码式 MySQL 标识
     * @return 连接元数据
     */
    private static ConnectMetaData parseUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        uri = uri.replaceAll("'", "").replaceAll("\"", "");
        ConnectMetaData metaData = new ConnectMetaData();
        int sharp = uri.indexOf('#');
        int at = uri.indexOf('@');
        int slash = uri.indexOf('/');
        if (sharp == -1 || at == -1 || slash == -1) {
            return null;
        }
        String type = uri.substring(0, sharp);
        if ("mysql".equalsIgnoreCase(type)) {
            metaData.setType(ConnectMetaData.Type.MYSQL);
        } else {
            return null;
        }

        String user = uri.substring(sharp + 1, at);
        if (user.isEmpty()) {
            return null;
        }
        int colon = user.indexOf(':');
        String username = user.substring(0, colon);
        String password = user.substring(colon + 1);
        if (username.isEmpty()) {
            return null;
        }
        metaData.setUsername(username);
        metaData.setPassword(password);

        String link = uri.substring(at + 1, slash);
        if (link.isEmpty()) {
            return null;
        }
        colon = link.indexOf(':');
        String host = link.substring(0, colon);
        String port = link.substring(colon + 1);
        if (host.isEmpty() || port.isEmpty()) {
            return null;
        }
        metaData.setHost(host);
        metaData.setPort(port);

        String db = uri.substring(slash + 1);
        if (db.isEmpty()) {
            return null;
        }
        metaData.setDatabase(db);

        return metaData;
    }

    private static Path parseFilepath(String filepath) {
        if (filepath == null || filepath.isEmpty()) {
            return null;
        }
        filepath = filepath.replaceAll("'", "").replaceAll("\"", "");
        Path path = Paths.get(filepath);
        if (!Files.exists(path)) {
            return null;
        }
        return path;
    }
}
