package com.flink.flinksql.connector.filestream;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class FileStreamConfigOption {
    // define all options statically
    public static final ConfigOption<String> FILEPATH = ConfigOptions.key("filepath").stringType().noDefaultValue();

    public static final ConfigOption<Integer>  MONITOR_GAP = ConfigOptions.key("monitor-gap").intType().noDefaultValue();

    public static final ConfigOption<Boolean> IS_RECURSIVE = ConfigOptions.key("is-recursive").booleanType().defaultValue(true);

    public static final ConfigOption<Boolean> IS_DATEPATH = ConfigOptions.key("is-datepath").booleanType().defaultValue(true);

    public static final ConfigOption<String> PARTITION_PATTERN = ConfigOptions.key("partition-pattern").stringType().noDefaultValue();

    public static final ConfigOption<String> PARTITION_DATEFORMAT = ConfigOptions.key("partition-dateformat").stringType().defaultValue("yyyyMMdd");

    public static final ConfigOption<String> FILE_PATTERN = ConfigOptions.key("file-pattern").stringType().defaultValue(".*");
}
