package de.unibi.agbi.biodwh2.sql.exporter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration {
    @JsonProperty("tableNameAlias")
    public final Map<String, String> tableNameAlias;

    public Configuration() {
        tableNameAlias = new HashMap<>();
    }
}
