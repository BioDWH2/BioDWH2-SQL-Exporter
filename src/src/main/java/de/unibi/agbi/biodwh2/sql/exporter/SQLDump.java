package de.unibi.agbi.biodwh2.sql.exporter;

import de.unibi.agbi.biodwh2.core.lang.Type;
import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.core.model.graph.IndexDescription;
import de.unibi.agbi.biodwh2.core.model.graph.Node;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

final class SQLDump {
    private int insertBatchSize = 100;
    private String schemaName = "biodwh2";
    private final BufferedWriter writer;
    private final Graph graph;

    public SQLDump(final BufferedWriter writer, final Graph graph) {
        this.writer = writer;
        this.graph = graph;
    }

    public void setInsertBatchSize(final int insertBatchSize) {
        this.insertBatchSize = Math.max(1, insertBatchSize);
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName == null ? null : schemaName.trim();
    }

    private String getSchemaPrefix() {
        return StringUtils.isBlank(schemaName) ? "" : "`" + schemaName + "`.";
    }

    public void write() throws IOException {
        writeSchema();
        writeData();
    }

    private void writeSchema() throws IOException {
        if (getSchemaPrefix().length() > 0) {
            writeLine("-- -----------------------------------------------------");
            writeLine("-- Schema " + schemaName);
            writeLine("-- -----------------------------------------------------");
            writeLine("CREATE SCHEMA IF NOT EXISTS `" + schemaName +
                      "` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;");
            writeLine("USE `" + schemaName + "`;");
        }
        writer.newLine();
        writeNodeTables();
        writeEdgeTables();
    }

    private void writeLine(final String line) throws IOException {
        writer.write(line);
        writer.newLine();
    }

    private void writeNodeTables() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Node tables");
        writeLine("-- -----------------------------------------------------");
        for (final String label : graph.getNodeLabels()) {
            writeLine("DROP TABLE IF EXISTS " + getSchemaPrefix() + "`" + label + "`;");
            writeLine("CREATE TABLE IF NOT EXISTS " + getSchemaPrefix() + "`" + label + "` (");
            for (final Map.Entry<String, Type> entry : graph.getPropertyKeyTypesForNodeLabel(label).entrySet()) {
                if ("__label".equals(entry.getKey()))
                    continue;
                writeLine("  `" + entry.getKey() + "` " + getSQLType(entry.getKey(), entry.getValue()) + " " +
                          getSQLTypeAttributes(entry.getKey(), entry.getValue()) + ",");
            }
            for (final IndexDescription index : graph.indexDescriptions())
                if (index.getTarget() == IndexDescription.Target.NODE && index.getLabel().equals(label)) {
                    final String indexType = index.getType() == IndexDescription.Type.UNIQUE ? "UNIQUE " : "";
                    final String indexName =
                            index.getProperty() + (index.getType() == IndexDescription.Type.UNIQUE ? "_UNIQUE" : "");
                    // TODO: writeLine("  " + indexType + "INDEX `" + indexName + "` (`" + index.getProperty() + "` ASC),");
                }
            writeLine("  PRIMARY KEY (`__id`),");
            writeLine("  UNIQUE INDEX `__id_UNIQUE` (`__id` ASC)");
            writeLine(");");
            writer.newLine();
        }
    }

    /**
     * https://dev.mysql.com/doc/refman/8.0/en/data-types.html
     */
    private String getSQLType(final String key, final Type type) {
        if (type.isList()) {
            return "JSON";
        } else {
            if ("__id".equals(key) || "__from_id".equals(key) || "__to_id".equals(key))
                return "BIGINT UNSIGNED";
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
            if (type.getType() == Boolean.class || type.getType() == Byte.class)
                return "TINYINT";
            // TODO
        }
        return "";
    }

    private String getSQLTypeAttributes(final String key, final Type type) {
        if ("__id".equals(key) || "__from_id".equals(key) || "__to_id".equals(key))
            return "NOT NULL";
        return "NULL";
    }

    private void writeEdgeTables() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Edge tables");
        writeLine("-- -----------------------------------------------------");
        // TODO
        writer.newLine();
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
        final String[] keys = propertyKeyTypes.keySet().stream().filter(k -> !"__label".equals(k)).toArray(
                String[]::new);
        final String keysString = Arrays.stream(keys).collect(Collectors.joining("`, `", "`", "`"));
        writeLine("INSERT INTO " + getSchemaPrefix() + "`" + label + "` (" + keysString + ") VALUES");
        for (int i = 0; i < batch.size(); i++) {
            final Node node = batch.get(i);
            final String values = Arrays.stream(keys).map(
                    key -> formatProperty(key, propertyKeyTypes.get(key), node.get(key))).collect(
                    Collectors.joining(", "));
            writeLine("  (" + values + ")" + (i < batch.size() - 1 ? "," : ";"));
        }
    }

    private String formatProperty(final String key, final Type type, final Object value) {
        if (value == null)
            return "NULL";
        if (type.isList()) {
            if (Collection.class.isAssignableFrom(type.getType())) {
                final Collection<?> collection = (Collection<?>) value;
                final String values = collection.stream().map(e -> formatProperty(type.getComponentType(), e, "\""))
                                                .collect(Collectors.joining(", "));
                return "'[" + escapeQuoting(values, "'") + "]'";
            } else if (type.getType().isArray()) {
                final Object[] array = (Object[]) value;
                final String values = Arrays.stream(array).map(e -> formatProperty(type.getComponentType(), e, "\""))
                                            .collect(Collectors.joining(", "));
                return "'[" + escapeQuoting(values, "'") + "]'";
            }
            // TODO
            return "";
        }
        return formatProperty(type.getType(), value, "'");
    }

    private String formatProperty(final Class<?> type, final Object value, final String quoteChar) {
        if (type == Long.class || type == Integer.class || type == Short.class || type == Byte.class)
            return value.toString();
        if (type == Boolean.class)
            return value.toString().toUpperCase(Locale.ROOT);
        return quoteChar + escapeQuoting(value.toString(), quoteChar) + quoteChar;
    }

    private String escapeQuoting(final String value, final String quoteChar) {
        return StringUtils.replace(value, quoteChar, '\\' + quoteChar);
    }

    private void writeEdgeData() throws IOException {

    }
}