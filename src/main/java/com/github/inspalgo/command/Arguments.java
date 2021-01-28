package com.github.inspalgo.command;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.logic.Dispatcher;
import com.github.inspalgo.logic.TargetMetaData;
import com.github.inspalgo.util.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

/**
 * @author InspAlgo
 * @date 2021/1/13 15:03 UTC+08:00
 */
@Command(name = "MySQL Schema Sync", mixinStandardHelpOptions = true, version = "MySQL Schema Sync v0.5")
public class Arguments implements Runnable {
    @Option(names = {"-s", "--source"}, description = "指定源：1.在线方式 -s mysql#username:password@host:port/database_name, 2.SQL文件方式  -s sql_filepath")
    private String source;

    @ArgGroup(exclusive = false, multiplicity = "0..*")
    private List<ConnectTarget> connectTargets;

    @ArgGroup(exclusive = false, multiplicity = "0..*")
    private List<FileTarget> fileTargets;

    @Option(names = {"-p", "--preview"}, description = "仅预览执行")
    private boolean preview = false;

    private static class ConnectTarget {
        @Option(names = {"-t", "--target"}, description = "指定目标：目前只有在线方式 -t mysql#username:password@host:port/database_name")
        private String connect;
        @Option(names = {"-o", "--output"}, description = "输出执行的差异DDL到指定文件中，-o filepath")
        private String outputFilepath;

        @Override
        public String toString() {
            return String.format("ConnectTarget{%s,%s}", connect, outputFilepath);
        }
    }

    private static class FileTarget {
        @Option(names = {"-f", "--file"}, description = "目标为 SQL 文件，将自动生成DDL文件到指定路径，-f sql_filepath ddl_filepath")
        private String fileSqlFilepath;
        @Parameters
        private String outputFilepath;

        @Override
        public String toString() {
            return String.format("FileTarget{%s,%s}", fileSqlFilepath, outputFilepath);
        }
    }

    @Override
    public void run() {
        try {
            new Dispatcher().setSource(getSource()).setTargetList(getTargetList())
                            .setPreview(preview).schemaSync();
        } catch (Exception e) {
            Log.COMMON.error("", e);
            System.exit(-1);
        }
    }

    private Object getSource() {
        Object sourceMetaData = null;

        if (source != null) {
            sourceMetaData = parseUri(source);
            if (sourceMetaData == null) {
                sourceMetaData = parseFilepath(source, FileLimitType.READ);
            }
            if (sourceMetaData == null) {
                Log.COMMON.error("源数据库资源标识格式错误: [{}]", source);
                System.exit(-1);
            }
        }

        return sourceMetaData;
    }

    private List<TargetMetaData> getTargetList() {
        List<ConnectTarget> connectTargetList = Optional.ofNullable(connectTargets).orElse(new ArrayList<>());
        List<FileTarget> fileTargetList = Optional.ofNullable(fileTargets).orElse(new ArrayList<>());
        List<TargetMetaData> targetList = new ArrayList<>(connectTargetList.size() + fileTargetList.size());

        for (ConnectTarget t : connectTargetList) {
            ConnectMetaData target = parseUri(t.connect);
            if (target == null) {
                Log.COMMON.error("目标数据库资源标识格式错误: [{}]", t);
                System.exit(-1);
            }
            Path outputPath = parseFilepath(t.outputFilepath, FileLimitType.WRITE);
            if (t.outputFilepath != null && outputPath == null) {
                Log.COMMON.error("目标数据库输出路径错误: [{}]", t);
                System.exit(-1);
            }
            targetList.add(new TargetMetaData(TargetMetaData.TargetType.CONNECT, target, outputPath));
        }

        for (FileTarget t : fileTargetList) {
            Path target = parseFilepath(t.fileSqlFilepath, FileLimitType.READ);
            Path outputPath = parseFilepath(t.outputFilepath, FileLimitType.WRITE);
            // 对于文件类型的目标，启动预览执行模式则不需要DDL输出文件
            if (target == null || (outputPath == null && preview == false)) {
                Log.COMMON.error("文件类型目标数据库格式错误: [{}]", t);
                System.exit(-1);
            }
            targetList.add(new TargetMetaData(TargetMetaData.TargetType.FILE, target, outputPath));
        }

        return targetList;
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

    private enum FileLimitType {
        /**
         * 具有读权限
         */
        READ,
        /**
         * 具有写权限
         */
        WRITE,
    }

    private Path parseFilepath(String filepath, FileLimitType type) {
        if (filepath == null || filepath.isEmpty()) {
            return null;
        }
        filepath = filepath.replaceAll("'", "").replaceAll("\"", "");
        Path path = Paths.get(filepath);
        if (type == FileLimitType.READ && Files.isReadable(path)) {
            return path;
        }
        if (type == FileLimitType.WRITE && preview == false) {
            try {
                if (!Files.exists(path)) {
                    Files.createFile(path);
                }
                if (Files.isWritable(path)) {
                    return path;
                }
            } catch (IOException e) {
                Log.COMMON.error("路径 {} 无法创建", filepath);
            }
        }
        return null;
    }
}
