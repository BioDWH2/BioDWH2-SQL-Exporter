![Java CI](https://github.com/BioDWH2/BioDWH2-SQL-Exporter/workflows/Java%20CI/badge.svg?branch=develop) ![Release](https://img.shields.io/github/v/release/BioDWH2/BioDWH2-SQL-Exporter) ![Downloads](https://img.shields.io/github/downloads/BioDWH2/BioDWH2-SQL-Exporter/total) ![License](https://img.shields.io/github/license/BioDWH2/BioDWH2-SQL-Exporter)

# BioDWH2-SQL-Exporter
| :warning: The SQL-Exporter is still experimental! For a stable solution, please try the [BioDWH2-Neo4j-Server](https://github.com/BioDWH2/BioDWH2-Neo4j-Server) or directly use the mapped GraphML file. |
| --- |

**BioDWH2** is an easy-to-use, automated, graph-based data warehouse and mapping tool for bioinformatics and medical informatics. The main repository can be found [here](https://github.com/BioDWH2/BioDWH2).

This repository contains the **BioDWH2-SQL-Exporter** utility which can be used to export a BioDWH2 graph database into a relational SQL database. There is no need for any SQL installation.

## Download
The latest release version of **BioDWH2-SQL-Exporter** can be downloaded [here](https://github.com/BioDWH2/BioDWH2-SQL-Exporter/releases/latest).

## Usage
BioDWH2-SQL-Exporter requires the Java Runtime Environment version 8. The JRE 8 is available [here](https://www.oracle.com/java/technologies/javase-jre8-downloads.html).

Creating an SQL database from any workspace is done using the following command. Every time the workspace is updated or changed, the create command has to be executed again.
~~~BASH
> java -jar BioDWH2-SQL-Exporter.jar --create /path/to/workspace
~~~

## Limitations
Depending on the selected SQL target DBMS, identifier length limitations need to be adhered to.

|       DBMS | Identifier length | Reference                                                                          |
|-----------:|------------------:|------------------------------------------------------------------------------------|
|      MySQL |                64 | [ref](https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html)              |
|    MariaDB |                64 | [ref](https://mariadb.com/kb/en/identifier-names/#maximum-length)                  |
| PostgreSQL |                63 | [ref](https://www.postgresql.org/docs/current/limits.html)                         |
|     sqlite |                 - | [ref](https://www.sqlite.org/limits.html)                                          |
|      MSSQL |               128 | [ref](https://www.c-sharpcorner.com/blogs/maximum-length-of-objects-in-sql-server) |

## Help
~~~
Usage: BioDWH2-SQL-Exporter.jar [-h] [-c=<workspacePath>]
                                [--insert-batch-size=<batchSize>]
                                [--schema-name=<schemaName>]
  -c, --create=<workspacePath>
               Create an SQL database from the workspace graph
  -h, --help   print this message
      --insert-batch-size=<batchSize>
               Batch size of insert statements (default: 100)
      --schema-name=<schemaName>
               SQL schema name (default: biodwh2)
      --target=<target>
               SQL DBMS target [mysql, mariadb, sqlite, postgresql, mssql] (default: mysql)
~~~