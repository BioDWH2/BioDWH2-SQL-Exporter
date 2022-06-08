package de.unibi.agbi.biodwh2.sql.exporter.model;

import picocli.CommandLine;

@CommandLine.Command(name = "BioDWH2-SQL-Exporter.jar")
public class CmdArgs {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this message", order = 1)
    public boolean help;
    @CommandLine.Option(names = {
            "-c", "--create"
    }, arity = "1", paramLabel = "<workspacePath>", description = "Create an SQL database from the workspace graph", order = 2)
    public String create;
    @CommandLine.Option(names = {
            "--insert-batch-size"
    }, arity = "1", paramLabel = "<batchSize>", description = "Batch size of insert statements (default: 100)", defaultValue = "100", order = 3)
    public Integer insertBatchSize;
    @CommandLine.Option(names = {
            "--schema-name"
    }, arity = "1", paramLabel = "<schemaName>", description = "SQL schema name (default: biodwh2)", defaultValue = "biodwh2", order = 4)
    public String schemaName;
    @CommandLine.Option(names = {
            "--target"
    }, arity = "1", paramLabel = "<target>", description = "SQL DBMS target [mysql, mariadb, sqlite, postgresql, mssql] (default: mysql)", defaultValue = "mysql", order = 5)
    public String target;
}
