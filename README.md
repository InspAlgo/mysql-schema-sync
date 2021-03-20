# mysql-schema-sync
MySQL 表结构同步工具  

## Introduction
这是一个基于 JDK8 构建的 MySQL 表结构同步工具。创建本项目的目的在于方便地进行自动化同步表结构，对于一些内部 CI/CD 时，由流水线自动执行会比较方便。
对于使用 Navicat GUI 工具在本地手动同步表结构同时项目本身也使用如 Jenkins 一类的 CI/CD 工具的开发者而言，这个表结构同步工具可能会比较有帮助。  

本工具的实现主要是基于 CREATE 语句进行比对，并生成差异的 DDL 语句，再执行这个 DDL 语句从而达到表结构同步的目的。和使用 Navicat 执行表结构同步的方式类似。  
由于未实现 Trigger 和 Online DDL 方式，所以不建议在大数据量场景下使用，一般在开发阶段用于同步开发库与测试库时使用。  

## Features  
- [x] 支持表的字段结构同步  
- [x] 支持主键同步  
- [x] 支持索引同步  
- [ ] 不支持外键同步  
- [ ] 不支持视图同步  
- [ ] 不支持 Trigger  
- [ ] 不支持 Online DDL  


## Usage
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


## Future development plans  

+ 能够使用 Trigger 方式同步表结构  
+ 能够使用 Online DDL 方式同步表结构  


## License
Copyright (c) [InspAlgo](https://github.com/InspAlgo). All rights reserved.  

Licensed under the [MIT](LICENSE) license.  