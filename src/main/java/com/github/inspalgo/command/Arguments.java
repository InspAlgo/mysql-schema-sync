package com.github.inspalgo.command;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.logic.Dispatcher;
import com.github.inspalgo.util.Log;

import java.util.ArrayList;
import java.util.List;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * @author InspAlgo
 * @date 2021/1/13 15:03 UTC+08:00
 */
@Command(name = "MySQL Schema Sync", mixinStandardHelpOptions = true, version = "MySQL Schema Sync v0.1")
public class Arguments implements Runnable {
    @Option(names = {"-s", "--source"}, required = true, description = "Source database.\nE.g. 'mysql#username:password@host:port/database_name' .")
    private String source;

    @Option(names = {"-t", "--target"}, required = true, description = "Target Database.\nE.g. 'mysql#username:password@host:port/database_name' .")
    private List<String> targets;

    @Option(names = {"-p", "--preview"}, description = "Executing Process Preview")
    private boolean preview = false;

    @Override
    public void run() {
        ConnectMetaData sourceConnectMetaData = null;
        List<ConnectMetaData> targetConnectMetaDataList = null;

        if (source != null) {
            sourceConnectMetaData = parseUri(source);
            if (sourceConnectMetaData != null) {
                Log.COMMON.info(sourceConnectMetaData.toString());
            } else {
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

            Log.COMMON.info(targetConnectMetaDataList.toString());
        }

        try {
            Dispatcher.onlineSchemaSync(sourceConnectMetaData, targetConnectMetaDataList, preview);
        } catch (Exception e) {
            Log.COMMON.error("", e);
            System.exit(-1);
        }
    }

    /**
     * 解析 mysql#username:password@host:port/database_name 形式的标识
     *
     * @param uri 在线密码式 MySQL 标识
     * @return 连接元数据
     */
    private ConnectMetaData parseUri(String uri) {
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
}
