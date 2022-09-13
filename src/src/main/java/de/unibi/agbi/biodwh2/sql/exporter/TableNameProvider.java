package de.unibi.agbi.biodwh2.sql.exporter;

import de.unibi.agbi.biodwh2.core.model.graph.Edge;
import de.unibi.agbi.biodwh2.core.model.graph.Graph;
import de.unibi.agbi.biodwh2.sql.exporter.model.Configuration;
import de.unibi.agbi.biodwh2.sql.exporter.model.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableNameProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableNameProvider.class);

    private final Target target;
    private final Configuration configuration;
    private final Map<String, Map<String, Set<String>>> edgeLabelNodeLabelsMap;

    TableNameProvider(final Configuration configuration, final Target target, final Graph graph) {
        this.configuration = configuration;
        this.target = target;
        edgeLabelNodeLabelsMap = new HashMap<>();
        for (final String label : graph.getNodeLabels())
            validateAliasConfigured(label);
        for (final String label : graph.getEdgeLabels()) {
            edgeLabelNodeLabelsMap.put(label, new HashMap<>());
            for (final Edge edge : graph.getEdges(label)) {
                final String fromLabel = graph.getNode(edge.getFromId()).getLabel();
                final String toLabel = graph.getNode(edge.getToId()).getLabel();
                final Set<String> toLabels = edgeLabelNodeLabelsMap.get(label).computeIfAbsent(fromLabel,
                                                                                               k -> new HashSet<>());
                if (!toLabels.contains(toLabel)) {
                    toLabels.add(toLabel);
                    validateAliasConfigured(getEdgeTableNameRaw(label, fromLabel, toLabel));
                }
            }
        }
    }

    private void validateAliasConfigured(final String name) {
        if (target != Target.Sqlite)
            validateAliasConfigured(name, target == Target.MSSQL ? 128 : 63);
    }

    private void validateAliasConfigured(final String name, final int limit) {
        if (name.length() > limit) {
            final String alias = configuration.tableNameAlias.get(name);
            if (alias == null) {
                LOGGER.warn("No alias provided for table name '" + name + "' exceeding identifier limit of " + limit +
                            " for target " + target);
                configuration.tableNameAlias.put(name, name);
            } else if (alias.length() > limit) {
                LOGGER.warn(
                        "Provided alias '" + alias + "' for table name '" + name + "' exceeds identifier limit of " +
                        limit + " for target " + target);
            }
        }
    }

    public Map<String, Set<String>> getEdgeLabelNodeLabelsMap(final String label) {
        return edgeLabelNodeLabelsMap.get(label);
    }

    public String getNodeTableName(final String label) {
        return aliasNameIfNecessary(label);
    }

    public String getEdgeTableName(final String label, final String fromLabel, final String toLabel) {
        return aliasNameIfNecessary(getEdgeTableNameRaw(label, fromLabel, toLabel));
    }

    private String getEdgeTableNameRaw(final String label, final String fromLabel, final String toLabel) {
        return fromLabel + "__" + label + "__" + toLabel;
    }

    private String aliasNameIfNecessary(final String name) {
        return target == Target.Sqlite ? name : aliasNameIfNecessary(name, target == Target.MSSQL ? 128 : 63);
    }

    private String aliasNameIfNecessary(final String name, final int limit) {
        if (name.length() <= limit)
            return name;
        final String alias = configuration.tableNameAlias.get(name);
        return alias == null ? name : alias;
    }
}
