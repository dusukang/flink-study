package com.flink.flinksql.catalog;

import java.util.Objects;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

public class MysqlTablePath {

    private static final String DEFAULT_MYSQL_DATABASE_NAME = "mysql";

    private final String mysqlDataBase;

    private final String mysqlTable;

    public MysqlTablePath(String mysqlDataBase, String mysqlTable) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(mysqlDataBase));
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(mysqlTable));
        this.mysqlDataBase = mysqlDataBase;
        this.mysqlTable = mysqlTable;
    }

    public static MysqlTablePath fromFlinkTableName(String flinkTableName) {
        if (!flinkTableName.contains(".")) {
            return new MysqlTablePath(DEFAULT_MYSQL_DATABASE_NAME, flinkTableName);
        } else {
            String[] path = flinkTableName.split("\\.");
            Preconditions.checkArgument(path != null && path.length == 2, String.format("Table name '%s' is not valid. The parsed length is %d", flinkTableName, path.length));
            return new MysqlTablePath(path[0], path[1]);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return (new MysqlTablePath(schema, table)).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", this.mysqlDataBase, this.mysqlTable);
    }

    public String getMysqlTableName() {
        return this.mysqlTable;
    }

    public String getMysqlDataBaseName() {
        return this.mysqlDataBase;
    }

    public String toString() {
        return this.getFullPath();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            MysqlTablePath that = (MysqlTablePath)o;
            return Objects.equals(this.mysqlDataBase, that.mysqlDataBase) && Objects.equals(this.mysqlTable, that.mysqlTable);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.mysqlDataBase, this.mysqlTable});
    }

}
