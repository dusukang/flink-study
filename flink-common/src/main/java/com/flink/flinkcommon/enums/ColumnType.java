package com.flink.flinkcommon.enums;

public enum ColumnType {
    /*
     * string
     */
    STRING,
    /**
     * varchar
     */
    VARCHAR,
    /**
     * char
     */
    CHAR,
    /**
     * int
     */
    INT,
    /**
     * mediumint
     */
    MEDIUMINT,
    /**
     * tinyint
     */
    TINYINT,
    /**
     * datetime
     */
    DATETIME,
    /**
     * smallint
     */
    SMALLINT,
    /**
     * bigint
     */
    BIGINT,
    /**
     * double
     */
    DOUBLE,
    /**
     * float
     */
    FLOAT,
    /**
     * boolean
     */
    BOOLEAN,
    /**
     * date
     */
    DATE,
    /**
     * timestamp
     */
    TIMESTAMP,
    /**
     * time eg: 23:59:59
     */
    TIME,
    /**
     * decimal
     */
    DECIMAL;

    public static ColumnType fromString(String type) {
        if(type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if(type.toUpperCase().startsWith("DECIMAL")) {
            return DECIMAL;
        }

        return valueOf(type.toUpperCase());
    }

}
