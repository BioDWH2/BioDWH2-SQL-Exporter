package de.unibi.agbi.biodwh2.sql.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.unibi.agbi.biodwh2.core.exceptions.WorkspaceException;
import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.core.net.BioDWH2Updater;
import de.unibi.agbi.biodwh2.sql.exporter.model.CmdArgs;
import de.unibi.agbi.biodwh2.sql.exporter.model.Configuration;
import de.unibi.agbi.biodwh2.sql.exporter.model.Target;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public class SQLExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExporter.class);
    private static final String CONFIG_FILE_NAME = "sql_config.json";

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
        exportSQL(workspacePath, commandLine.insertBatchSize, commandLine.schemaName,
                  parseTargetSafe(commandLine.target));
        storeWorkspaceHash(workspacePath);
        LOGGER.info("SQL database successfully created.");
    }

    private Target parseTargetSafe(final String target) {
        if (target == null)
            return Target.DEFAULT;
        switch (target.toLowerCase(Locale.ROOT)) {
            case "mssql":
                return Target.MSSQL;
            case "sqlite":
                return Target.Sqlite;
            case "mariadb":
                return Target.MariaDB;
            case "postgresql":
                return Target.Postgresql;
            case "mysql":
            default:
                return Target.MySQL;
        }
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

    private void exportSQL(final String workspacePath, final Integer insertBatchSize, final String schemaName,
                           final Target target) {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Creating sql dump for target " + target + "...");
        Path databasePath = Paths.get(workspacePath, "sql", "dump.sql");
        try (final OutputStream stream = Files.newOutputStream(databasePath);
             final OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
             final BufferedWriter writer = new BufferedWriter(streamWriter); final Graph graph = new Graph(
                Paths.get(workspacePath, "sources/mapped.db"), true, true)) {
            final Configuration configuration = createOrLoadConfiguration(workspacePath);
            final TableNameProvider tableNameProvider = new TableNameProvider(configuration, target, graph);
            saveConfiguration(workspacePath, configuration);
            final SQLDump dump = new SQLDump(writer, graph);
            if (insertBatchSize != null)
                dump.setInsertBatchSize(insertBatchSize);
            if (schemaName != null)
                dump.setSchemaName(schemaName);
            dump.setTarget(target);
            dump.write(tableNameProvider);
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Failed to create sql database '" + databasePath + "'", e);
        }
    }

    private Configuration createOrLoadConfiguration(final String workspacePath) {
        try {
            final Configuration configuration = loadConfiguration(workspacePath);
            return configuration == null ? createConfiguration(workspacePath) : configuration;
        } catch (IOException e) {
            throw new WorkspaceException("Failed to load or create workspace SQL configuration", e);
        }
    }

    private Configuration loadConfiguration(final String workspacePath) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final Path path = getConfigurationFilePath(workspacePath);
        return Files.exists(path) ? objectMapper.readValue(path.toFile(), Configuration.class) : null;
    }

    private Path getConfigurationFilePath(final String workspacePath) {
        return Paths.get(workspacePath, "sql", CONFIG_FILE_NAME);
    }

    private Configuration createConfiguration(final String workspacePath) throws IOException {
        final Configuration configuration = new Configuration();
        saveConfiguration(workspacePath, configuration);
        return configuration;
    }

    private void saveConfiguration(final String workspacePath, final Configuration configuration) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.writeValue(getConfigurationFilePath(workspacePath).toFile(), configuration);
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
