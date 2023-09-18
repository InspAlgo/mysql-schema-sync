package com.github.inspalgo.command;

import com.github.inspalgo.core.ConnectMetaData;
import com.github.inspalgo.logic.Dispatcher;
import com.github.inspalgo.logic.TargetMetaData;
import com.github.inspalgo.util.Log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.IVersionProvider;
import static picocli.CommandLine.Option;

/**
 * @author InspAlgo
 * @date 2021/1/13 15:03 UTC+08:00
 */
@Command(name = "MySQL Schema Sync", versionProvider = Arguments.VersionProvider.class)
public class Arguments implements Runnable {
    @Option(names = {"-v", "--version"}, versionHelp = true, description = "显示版本号并退出")
    private boolean versionRequested;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "显示帮助信息")
    boolean usageHelpRequested;

    @Option(names = {"-s", "--source"}, description = "指定源：1.在线方式 -s mysql#username:password@host:port/database_name, 2.SQL文件方式  -s sql_filepath")
    private String source;

    @ArgGroup(exclusive = false, multiplicity = "0..*")
    private List<Target> targets;

    @Option(names = {"-p", "--preview"}, description = "仅预览执行")
    private boolean preview;

    @Option(names = {"-r", "--recreate-table-on-error"}, description = "在同步表结构失败时重新创建表")
    private boolean recreateTableOnError;

    @Override
    public void run() {
        try {
            new Dispatcher().setSource(getSource()).setTargetList(getTargetList())
                            .setPreview(preview).setRecreateTableOnError(recreateTableOnError)
                            .schemaSync();
        } catch (Exception e) {
            Log.COMMON.error("", e);
            System.exit(-1);
        }
    }

    static class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            try {
                final Properties properties = new Properties();
                properties.load(Arguments.class.getClassLoader().getResourceAsStream("project.properties"));
                return new String[]{properties.getProperty("version")};
            } catch (Exception e) {
                return new String[]{"Unable to read version from project.properties."};
            }
        }
    }

    private static class Target {
        @Option(names = {"-t", "--target"}, description = "指定目标：1.在线方式 -t mysql#username:password@host:port/database_name, 2.SQL文件方式  -t sql_filepath")
        private String target;
        @Option(names = {"-o", "--output"}, description = "输出执行的差异DDL到指定文件中，-o filepath")
        private String outputFilepath;

        @Override
        public String toString() {
            return String.format("Target{%s,%s}", target, outputFilepath);
        }
    }

    private Object getSource() {
        Object sourceMetaData = Optional.ofNullable((Object) parseUri(source))
                                        .orElseGet(() -> parseFilepath(source, FileLimitType.READ));
        if (source != null && sourceMetaData == null) {
            Log.COMMON.error("源数据库资源标识格式错误: [{}]", source);
            System.exit(-1);
        }
        return sourceMetaData;
    }

    private List<TargetMetaData> getTargetList() {
        List<Target> connectTargetList = Optional.ofNullable(targets).orElse(new ArrayList<>());
        List<TargetMetaData> targetList = new ArrayList<>(connectTargetList.size());

        for (Target t : connectTargetList) {
            Object target = Optional.ofNullable((Object) parseUri(t.target))
                                    .orElseGet(() -> parseFilepath(t.target, FileLimitType.READ));
            if (target == null) {
                Log.COMMON.error("目标数据库资源标识格式错误: [{}]", t);
                System.exit(-1);
            }
            Path outputPath = parseFilepath(t.outputFilepath, FileLimitType.WRITE);
            if (t.outputFilepath != null && outputPath == null) {
                Log.COMMON.error("目标数据库输出路径错误: [{}]", t);
                System.exit(-1);
            }
            targetList.add(new TargetMetaData(target, outputPath));
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
        int at = uri.lastIndexOf("@");
        int slash = uri.indexOf('/', at);
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
        Path path = null;
        try {
            filepath = filepath.replaceAll("'", "").replaceAll("\"", "").trim();
            path = Paths.get(filepath);
            if (type == FileLimitType.READ) {
                if (!Files.isReadable(path)) {
                    path = null;
                }
            } else if (type == FileLimitType.WRITE) {
                if (!Files.exists(path)) {
                    Files.createFile(path);
                }
                if (!Files.isWritable(path)) {
                    path = null;
                }
            } else {
                path = null;
            }
        } catch (Exception e) {
            Log.COMMON.error("路径 [{}] 不正确，请检查权限", filepath);
        }
        return path;
    }
}
