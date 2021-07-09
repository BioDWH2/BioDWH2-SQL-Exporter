package de.unibi.agbi.biodwh2.sql.exporter.model;

import picocli.CommandLine;

@CommandLine.Command(name = "BioDWH2-SQL-Exporter.jar")
public class CmdArgs {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this message")
    public boolean help;
    @CommandLine.Option(names = {
            "-c", "--create"
    }, arity = "1", paramLabel = "<workspacePath>", description = "Create an SQL database from the workspace graph")
    public String create;
}
