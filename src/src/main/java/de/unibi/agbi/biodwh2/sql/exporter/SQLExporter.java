package de.unibi.agbi.biodwh2.sql.exporter;

import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.core.net.BioDWH2Updater;
import de.unibi.agbi.biodwh2.sql.exporter.model.CmdArgs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SQLExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExporter.class);

    private SQLExporter() {
    }

    public static void main(String... args) {
        final CmdArgs commandLine = parseCommandLine(args);
        new SQLExporter().run(commandLine);
    }

    private static CmdArgs parseCommandLine(final String... args) {
        final CmdArgs result = new CmdArgs();
        final CommandLine cmd = new CommandLine(result);
        cmd.parseArgs(args);
        return result;
    }

    private void run(final CmdArgs commandLine) {
        BioDWH2Updater.checkForUpdate("BioDWH2-SQL-Exporter",
                                      "https://api.github.com/repos/BioDWH2/BioDWH2-SQL-Exporter/releases");
        if (commandLine.create != null)
            createWorkspaceDatabase(commandLine);
        else
            printHelp(commandLine);
    }

    private void createWorkspaceDatabase(final CmdArgs commandLine) {
        final String workspacePath = commandLine.create;
        if (!verifyWorkspaceExists(workspacePath)) {
            printHelp(commandLine);
            return;
        }
        //noinspection ResultOfMethodCallIgnored
        Paths.get(workspacePath, "sql").toFile().mkdir();
        exportSQL(workspacePath);
        storeWorkspaceHash(workspacePath);
        LOGGER.info("SQL database successfully created.");
    }

    private boolean verifyWorkspaceExists(final String workspacePath) {
        if (StringUtils.isEmpty(workspacePath) || !Paths.get(workspacePath).toFile().exists()) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Workspace path '" + workspacePath + "' was not found");
            return false;
        }
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Using workspace directory '" + workspacePath + "'");
        return true;
    }

    private void exportSQL(final String workspacePath) {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Creating sql database...");
        Path databasePath = Paths.get(workspacePath, "sql", "dump.sql");
        try (final OutputStream stream = Files.newOutputStream(databasePath);
             final OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
             final BufferedWriter writer = new BufferedWriter(streamWriter); final Graph graph = new Graph(
                Paths.get(workspacePath, "sources/mapped.db"), true, true)) {
            new SQLDump(writer, graph).write();
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Failed to create sql database '" + databasePath + "'", e);
        }
    }

    private void storeWorkspaceHash(final String workspacePath) {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Updating workspace sql cache checksum...");
        final Path hashFilePath = Paths.get(workspacePath, "sql/checksum.txt");
        try {
            final String hash = HashUtils.getFastPseudoHashFromFile(
                    Paths.get(workspacePath, "sources/mapped.db").toString());
            final FileWriter writer = new FileWriter(hashFilePath.toFile());
            writer.write(hash);
            writer.close();
        } catch (IOException e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Failed to store hash of workspace mapped graph", e);
        }
    }

    private void printHelp(final CmdArgs commandLine) {
        CommandLine.usage(commandLine, System.out);
    }
}
