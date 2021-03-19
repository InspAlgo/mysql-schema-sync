# mysql-schema-sync
MySQL 表结构同步工具

## Features
-[x] 支持表的字段结构同步
-[x] 支持主键同步
-[x] 支持索引同步
-[ ] 不支持外键同步
-[ ] 不支持视图同步


## How to use
从 [releases](https://github.com/InspAlgo/mysql-schema-sync/releases)  下载最新的 jar 文件（由 JDK8 编译构建）。

基本信息介绍
```
$ java -jar mysql-schema-sync.jar -v
v0.5.4

$ java -jar mysql-schema-sync.jar -h
Usage: MySQL Schema Sync [-hpv] [-s=<source>] [[-t=<target>]
                         [-o=<outputFilepath>]]...
  -h, --help              显示帮助信息
  -o, --output=<outputFilepath>
                          输出执行的差异DDL到指定文件中，-o filepath
  -p, --preview           仅预览执行
  -s, --source=<source>   指定源：1.在线方式 -s mysql#username:password@host:
                            port/database_name, 2.SQL文件方式  -s sql_filepath
  -t, --target=<target>   指定目标：1.在线方式 -t mysql#username:password@host:
                            port/database_name, 2.SQL文件方式  -t sql_filepath
  -v, --version           显示版本号并退出
```


同步表结构方式
```
# 同步的源为 mysqldump 导出的表结构 sql 文件，被同步的为在线的库
$ java -jar mysql-schema-sync.jar -s dump.sql -t mysql#root:root@127.0.0.1:3306/test_db

# 使用 -p 参数开启预览模式，仅显示生成的同步 DDL 语句
$ java -jar mysql-schema-sync.jar -s dump.sql -t mysql#root:root@127.0.0.1:3306/test_db -p

# 由一个源库向多个目标库同步
$ java -jar mysql-schema-sync.jar -s mysql#root:root@127.0.0.1:3306/source_db -t mysql#root:root@127.0.0.1:3306/target_db_a -t mysql#root:root@127.0.0.1:3306/target_db_b

# 使用 -o 参数输出 DDL 语句
$ java -jar mysql-schema-sync.jar -s mysql#root:root@127.0.0.1:3306/source_db -t mysql#root:root@127.0.0.1:3306/target_db_a -o ddl_1.sql -t mysql#root:root@127.0.0.1:3306/target_db_b -o ddl_2.sql
```

> 注：若没有参数，则会抛出异常。同时由于解析 mysql#username:password@host:
port/database_name 时，会根据「#、@、/」符号进行分隔，请 username、password、database_name 中尽量不要携带这些字符（password 中带有「#、@」无妨）。 

