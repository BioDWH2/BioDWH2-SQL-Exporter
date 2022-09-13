package de.unibi.agbi.biodwh2.sql.exporter.model;

public enum Target {
    MySQL,
    Sqlite,
    MSSQL,
    Postgresql,
    MariaDB;

    public static Target DEFAULT = Target.MySQL;
}
