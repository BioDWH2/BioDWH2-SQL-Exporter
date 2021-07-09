package de.unibi.agbi.biodwh2.sql.exporter;

import de.unibi.agbi.biodwh2.core.lang.Type;
import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.core.model.graph.Node;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

final class SQLDump {
    private final int insertBatchSize = 100;
    private final String schemaName = "biodwh2";
    private final BufferedWriter writer;
    private final Graph graph;

    public SQLDump(final BufferedWriter writer, final Graph graph) {
        this.writer = writer;
        this.graph = graph;
    }

    public void write() throws IOException {
        writeSchema();
        writeData();
    }

    private void writeSchema() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Schema " + schemaName);
        writeLine("-- -----------------------------------------------------");
        writeLine("CREATE SCHEMA IF NOT EXISTS `" + schemaName +
                  "` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;");
        writeLine("USE `" + schemaName + "`;");
        writer.newLine();
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Node tables");
        writeLine("-- -----------------------------------------------------");
        for (final String label : graph.getNodeLabels()) {
            writeLine("DROP TABLE IF EXISTS `" + schemaName + "`.`" + label + "`;");
            writeLine("CREATE TABLE IF NOT EXISTS `" + schemaName + "`.`" + label + "` (");
            for (final Map.Entry<String, Type> entry : graph.getPropertyKeyTypesForNodeLabel(label).entrySet()) {
                writeLine("  `" + entry.getKey() + "` " + getSQLType(entry.getKey(), entry.getValue()) + " NULL,");
            }
            // TODO indices
            writeLine("  PRIMARY KEY (`__id`),");
            writeLine("  UNIQUE INDEX `__id_UNIQUE` (`__id` ASC))");
            writeLine(") ENGINE = InnoDB;");
            writer.newLine();
        }
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Edge tables");
        writeLine("-- -----------------------------------------------------");
        // TODO
        writer.newLine();
    }

    private void writeLine(final String line) throws IOException {
        writer.write(line);
        writer.newLine();
    }

    /**
     * https://dev.mysql.com/doc/refman/8.0/en/data-types.html
     */
    private String getSQLType(final String key, final Type type) {
        if (type.isList()) {
            // TODO
        } else {
            if ("__id".equals(key) || "__from_id".equals(key) || "__to_id".equals(key))
                return "UNSIGNED BIGINT";
            if ("__label".equals(key))
                return "VARCHAR(128)";
            if (type.getType() == String.class)
                return "MEDIUMTEXT";
            if (type.getType() == Integer.class)
                return "INT";
            if (type.getType() == Long.class)
                return "BIGINT";
            if (type.getType() == Short.class)
                return "SMALLINT";
            if (type.getType() == Float.class)
                return "FLOAT";
            if (type.getType() == Double.class)
                return "DOUBLE";
            // TODO
        }
        return "";
    }

    private void writeData() throws IOException {
        writeNodeData();
        writeEdgeData();
    }

    private void writeNodeData() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Node data");
        writeLine("-- -----------------------------------------------------");
        writer.newLine();
        for (final String label : graph.getNodeLabels()) {
            writeLine("-- -----------------------------------------------------");
            writeLine("-- Node data for label " + label);
            writeLine("-- -----------------------------------------------------");
            writer.newLine();
            final List<Node> batch = new ArrayList<>();
            for (final Node node : graph.getNodes(label)) {
                batch.add(node);
                if (batch.size() == insertBatchSize) {
                    writeInsertBatch(label, batch);
                    batch.clear();
                }
            }
            if (batch.size() > 0) {
                writeInsertBatch(label, batch);
                batch.clear();
            }
            // TODO
            writer.newLine();
        }
    }

    private void writeInsertBatch(final String label, final List<Node> batch) throws IOException {
        final Map<String, Type> propertyKeyTypes = graph.getPropertyKeyTypesForNodeLabel(label);
        final String[] keys = propertyKeyTypes.keySet().toArray(new String[0]);
        final String keysString = Arrays.stream(keys).collect(Collectors.joining("`, `", "`", "`"));
        writeLine("INSERT INTO `" + schemaName + "`.`" + label + "` (" + keysString + ") VALUES");
        for (int i = 0; i < batch.size(); i++) {
            final Node node = batch.get(i);
            // TODO: proper quoting
            final String values = Arrays.stream(keys).map(node::get).map(o -> o != null ? "\"" + o + "\"" : "NULL")
                                        .collect(Collectors.joining(", "));
            writeLine("  (" + values + ")" + (i < batch.size() - 1 ? "," : ";"));
        }
    }

    private void writeEdgeData() throws IOException {

    }
}
