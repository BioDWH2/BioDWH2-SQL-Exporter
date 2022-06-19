package de.unibi.agbi.biodwh2.sql.exporter;

import de.unibi.agbi.biodwh2.core.lang.Type;
import de.unibi.agbi.biodwh2.core.model.graph.Edge;
import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.core.model.graph.IndexDescription;
import de.unibi.agbi.biodwh2.core.model.graph.Node;
import de.unibi.agbi.biodwh2.sql.exporter.model.Target;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

final class SQLDump {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExporter.class);

    private int insertBatchSize = 100;
    private String schemaName = "biodwh2";
    private final BufferedWriter writer;
    private final Graph graph;
    private Target target = Target.MySQL;

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

    public void setTarget(final Target target) {
        this.target = target;
    }

    private String escapeIdentifier(final String identifier) {
        if (StringUtils.isBlank(identifier))
            return "";
        if (target == Target.Postgresql)
            return '"' + identifier + '"';
        return '`' + identifier + '`';
    }

    private String getSchemaPrefix() {
        return StringUtils.isBlank(schemaName) ? "" : escapeIdentifier(schemaName) + '.';
    }

    public void write() throws IOException {
        writeDisableForeignKeys();
        writeSchema();
        writeData();
        writeEnableForeignKeys();
    }

    private void writeDisableForeignKeys() throws IOException {
        if (target == Target.Postgresql)
            writeLine("SET session_replication_role = 'replica';");
        else if (target == Target.Sqlite)
            writeLine("PRAGMA foreign_keys = 0;");
        else
            writeLine("SET FOREIGN_KEY_CHECKS = 0;");
    }

    private void writeEnableForeignKeys() throws IOException {
        if (target == Target.Postgresql)
            writeLine("SET session_replication_role = 'origin';");
        else if (target == Target.Sqlite)
            writeLine("PRAGMA foreign_keys = 1;");
        else
            writeLine("SET FOREIGN_KEY_CHECKS = 1;");
    }

    private void writeSchema() throws IOException {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Exporting schema...");
        if (StringUtils.isNotBlank(schemaName)) {
            writeLine("-- -----------------------------------------------------");
            writeLine("-- Schema " + schemaName);
            writeLine("-- -----------------------------------------------------");
            if (target == Target.Postgresql) {
                writeLine("DROP DATABASE IF EXISTS " + escapeIdentifier(schemaName) + ";");
                writeLine("CREATE DATABASE " + escapeIdentifier(schemaName) +
                          " WITH ENCODING 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';");
            } else if (target == Target.Sqlite) {
                writeLine("PRAGMA encoding = 'UTF-8';");
            } else {
                writeLine("CREATE SCHEMA IF NOT EXISTS " + escapeIdentifier(schemaName) +
                          " DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;");
            }
            if (target != Target.Sqlite)
                writeLine("USE " + escapeIdentifier(schemaName) + ";");
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
        int nodeTableIndex = 1;
        for (final String label : graph.getNodeLabels()) {
            final String labelFQDN = getFQDN(label);
            writeLine("DROP TABLE IF EXISTS " + labelFQDN + ";");
            writeLine("CREATE TABLE IF NOT EXISTS " + labelFQDN + " (");
            for (final Map.Entry<String, Type> entry : graph.getPropertyKeyTypesForNodeLabel(label).entrySet()) {
                if ("__label".equals(entry.getKey()))
                    continue;
                final boolean isIndexed = isIndexedProperty(label, IndexDescription.Target.NODE, entry.getKey());
                final String sqlDataType = getSQLType(entry.getKey(), entry.getValue(), isIndexed);
                writeLine("  " + escapeIdentifier(entry.getKey()) + " " + sqlDataType + " " +
                          getSQLTypeAttributes(entry.getKey()) + ",");
            }
            writeLine("  PRIMARY KEY (" + escapeIdentifier("__id") + ")");
            writeLine(");");
            for (final IndexDescription index : graph.indexDescriptions())
                if (index.getTarget() == IndexDescription.Target.NODE && index.getLabel().equals(label)) {
                    // MySQL does not support JSON array indices, so they are skipped
                    if (index.isArrayProperty() && target == Target.MySQL)
                        continue;
                    final String indexType = index.getType() == IndexDescription.Type.UNIQUE ? "UNIQUE " : "";
                    final String indexName = "index_n" + nodeTableIndex +
                                             (index.getType() == IndexDescription.Type.UNIQUE ? "_UNIQUE" : "");
                    nodeTableIndex++;
                    writeLine(
                            "CREATE " + indexType + "INDEX " + escapeIdentifier(indexName) + " ON " + labelFQDN + "(" +
                            escapeIdentifier(index.getProperty()) + " ASC);");
                }
            writeLine("CREATE UNIQUE INDEX " + escapeIdentifier("index_n" + nodeTableIndex + "_UNIQUE") + " ON " +
                      labelFQDN + "(" + escapeIdentifier("__id") + " ASC);");
            nodeTableIndex++;
            writer.newLine();
        }
    }

    private boolean isIndexedProperty(final String label, final IndexDescription.Target target,
                                      final String propertyKey) {
        for (final IndexDescription index : graph.indexDescriptions())
            if (index.getTarget() == target && index.getLabel().equals(label) && index.getProperty().equals(
                    propertyKey))
                return true;
        return false;
    }

    private String getFQDN(final String identifier) {
        if (target == Target.Sqlite)
            return escapeIdentifier(identifier);
        return getSchemaPrefix() + escapeIdentifier(identifier);
    }

    /**
     * https://dev.mysql.com/doc/refman/8.0/en/data-types.html
     */
    private String getSQLType(final String key, final Type type, final boolean isIndexed) {
        if (type.isList()) {
            return "JSON";
        } else {
            if ("__id".equals(key) || "__from_id".equals(key) || "__to_id".equals(key))
                return "BIGINT UNSIGNED";
            if ("__label".equals(key))
                return "VARCHAR(128)";
            if (CharSequence.class.isAssignableFrom(type.getType()))
                return isIndexed ? "VARCHAR(1024)" : "MEDIUMTEXT";
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
        }
        if (LOGGER.isWarnEnabled())
            LOGGER.warn("Failed to retrieve SQL type for key '" + key + "' and type '" + type.getType());
        return "";
    }

    private String getSQLTypeAttributes(final String key) {
        if ("__id".equals(key) || "__from_id".equals(key) || "__to_id".equals(key))
            return "NOT NULL";
        return "NULL";
    }

    private void writeEdgeTables() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Edge tables");
        writeLine("-- -----------------------------------------------------");
        int edgeTableIndexCounter = 1;
        for (final String label : graph.getEdgeLabels()) {
            final Map<String, Set<String>> fromToLabelsMap = new HashMap<>();
            for (final Edge edge : graph.getEdges(label)) {
                final String fromLabel = graph.getNode(edge.getFromId()).getLabel();
                final String toLabel = graph.getNode(edge.getToId()).getLabel();
                fromToLabelsMap.computeIfAbsent(fromLabel, k -> new HashSet<>()).add(toLabel);
            }
            final Map<String, Type> propertyKeyTypes = graph.getPropertyKeyTypesForEdgeLabel(label);
            for (final String fromLabel : fromToLabelsMap.keySet()) {
                for (final String toLabel : fromToLabelsMap.get(fromLabel)) {
                    edgeTableIndexCounter = writeEdgeTable(label, fromLabel, toLabel, propertyKeyTypes,
                                                           edgeTableIndexCounter);
                }
            }
        }
        writer.newLine();
    }

    private int writeEdgeTable(final String label, final String fromLabel, final String toLabel,
                               final Map<String, Type> propertyKeyTypes, int edgeTableIndexCounter) throws IOException {
        final String tableName = getEdgeTableName(label, fromLabel, toLabel);
        final String tableNameFQDN = getFQDN(tableName);
        writeLine("DROP TABLE IF EXISTS " + tableNameFQDN + ";");
        writeLine("CREATE TABLE IF NOT EXISTS " + tableNameFQDN + " (");
        for (final Map.Entry<String, Type> entry : propertyKeyTypes.entrySet()) {
            if ("__label".equals(entry.getKey()))
                continue;
            final boolean isIndexed = isIndexedProperty(label, IndexDescription.Target.EDGE, entry.getKey());
            final String sqlDataType = getSQLType(entry.getKey(), entry.getValue(), isIndexed);
            writeLine("  " + escapeIdentifier(entry.getKey()) + " " + sqlDataType + " " +
                      getSQLTypeAttributes(entry.getKey()) + ",");
        }
        writeLine("  PRIMARY KEY (" + escapeIdentifier("__id") + "),");
        writeLine("  FOREIGN KEY (" + escapeIdentifier("__from_id") + ") REFERENCES " + getFQDN(fromLabel) + "(" +
                  escapeIdentifier("__id") + "),");
        writeLine("  FOREIGN KEY (" + escapeIdentifier("__to_id") + ") REFERENCES " + getFQDN(toLabel) + "(" +
                  escapeIdentifier("__id") + ")");
        writeLine(");");
        for (final IndexDescription index : graph.indexDescriptions())
            if (index.getTarget() == IndexDescription.Target.EDGE && index.getLabel().equals(label)) {
                // MySQL does not support JSON array indices, so they are skipped
                if (index.isArrayProperty() && target == Target.MySQL)
                    continue;
                final String indexType = index.getType() == IndexDescription.Type.UNIQUE ? "UNIQUE " : "";
                final String indexName = "index_e" + edgeTableIndexCounter +
                                         (index.getType() == IndexDescription.Type.UNIQUE ? "_UNIQUE" : "");
                edgeTableIndexCounter++;
                writeLine(
                        "CREATE " + indexType + "INDEX " + escapeIdentifier(indexName) + " ON " + tableNameFQDN + "(" +
                        escapeIdentifier(index.getProperty()) + " ASC);");
            }
        writeLine("CREATE UNIQUE INDEX " + escapeIdentifier("index_e" + edgeTableIndexCounter + "_UNIQUE") + " ON " +
                  tableNameFQDN + "(" + escapeIdentifier("__id") + " ASC);");
        edgeTableIndexCounter++;
        writer.newLine();
        return edgeTableIndexCounter;
    }

    private String getEdgeTableName(final String label, final String fromLabel, final String toLabel) {
        final String tableName = fromLabel + "__" + label + "__" + toLabel;
        if (tableName.length() <= 52)
            return tableName;
        final StringBuilder builder = new StringBuilder();
        final String[] fromLabelParts = StringUtils.split(fromLabel, "_", 2);
        if (fromLabelParts.length == 2) {
            for (final char c : fromLabelParts[0].toCharArray())
                if (!Character.isLowerCase(c))
                    builder.append(c);
            builder.append('_').append(fromLabelParts[1]);
        } else
            builder.append(fromLabel);
        builder.append("__").append(label).append("__");
        final String[] toLabelParts = StringUtils.split(toLabel, "_", 2);
        if (toLabelParts.length == 2) {
            for (final char c : toLabelParts[0].toCharArray())
                if (!Character.isLowerCase(c))
                    builder.append(c);
            builder.append('_').append(toLabelParts[1]);
        } else
            builder.append(toLabel);
        return builder.toString();
    }

    private void writeData() throws IOException {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("Exporting data...");
        writeNodeData();
        writeEdgeData();
    }

    private void writeNodeData() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Node data");
        writeLine("-- -----------------------------------------------------");
        writer.newLine();
        for (final String label : graph.getNodeLabels()) {
            if (LOGGER.isInfoEnabled())
                LOGGER.info("Exporting nodes with label " + label + "...");
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
            writer.newLine();
        }
    }

    private void writeInsertBatch(final String label, final List<Node> batch) throws IOException {
        final Map<String, Type> propertyKeyTypes = graph.getPropertyKeyTypesForNodeLabel(label);
        final String[] keys = propertyKeyTypes.keySet().stream().filter(k -> !"__label".equals(k)).toArray(
                String[]::new);
        final String keysString = Arrays.stream(keys).map(this::escapeIdentifier).collect(
                Collectors.joining(", ", "", ""));
        writeLine("INSERT INTO " + getFQDN(label) + " (" + keysString + ") VALUES");
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
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("Failed to format list property for key '" + key + "' and type '" + type.getType());
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
        if (target == Target.Postgresql || target == Target.Sqlite) {
            return StringUtils.replace(value, quoteChar, quoteChar + quoteChar);
        }
        return StringUtils.replace(value, quoteChar, '\\' + quoteChar);
    }

    private void writeEdgeData() throws IOException {
        writeLine("-- -----------------------------------------------------");
        writeLine("-- Edge data");
        writeLine("-- -----------------------------------------------------");
        writer.newLine();
        for (final String label : graph.getEdgeLabels()) {
            if (LOGGER.isInfoEnabled())
                LOGGER.info("Exporting edges with label " + label + "...");
            writeLine("-- -----------------------------------------------------");
            writeLine("-- Edge data for label " + label);
            writeLine("-- -----------------------------------------------------");
            writer.newLine();
            final Map<String, List<Edge>> batches = new HashMap<>();
            for (final Edge edge : graph.getEdges(label)) {
                final String fromLabel = graph.getNode(edge.getFromId()).getLabel();
                final String toLabel = graph.getNode(edge.getToId()).getLabel();
                final String labelKey = fromLabel + "|" + toLabel;
                final List<Edge> batch = batches.computeIfAbsent(labelKey, k -> new ArrayList<>());
                batch.add(edge);
                if (batch.size() == insertBatchSize) {
                    writeInsertBatch(label, getEdgeTableName(label, fromLabel, toLabel), batch);
                    batch.clear();
                }
            }
            for (final String labelKey : batches.keySet()) {
                final List<Edge> batch = batches.get(labelKey);
                if (batch.size() > 0) {
                    final String[] labelParts = StringUtils.split(labelKey, "|", 2);
                    writeInsertBatch(label, getEdgeTableName(label, labelParts[0], labelParts[1]), batch);
                    batch.clear();
                }
            }
            writer.newLine();
        }
    }

    private void writeInsertBatch(final String label, final String tableLabel,
                                  final List<Edge> batch) throws IOException {
        final Map<String, Type> propertyKeyTypes = graph.getPropertyKeyTypesForEdgeLabel(label);
        final String[] keys = propertyKeyTypes.keySet().stream().filter(k -> !"__label".equals(k)).toArray(
                String[]::new);
        final String keysString = Arrays.stream(keys).map(this::escapeIdentifier).collect(
                Collectors.joining(", ", "", ""));
        writeLine("INSERT INTO " + getFQDN(tableLabel) + " (" + keysString + ") VALUES");
        for (int i = 0; i < batch.size(); i++) {
            final Edge edge = batch.get(i);
            final String values = Arrays.stream(keys).map(
                    key -> formatProperty(key, propertyKeyTypes.get(key), edge.get(key))).collect(
                    Collectors.joining(", "));
            writeLine("  (" + values + ")" + (i < batch.size() - 1 ? "," : ";"));
        }
    }
}
