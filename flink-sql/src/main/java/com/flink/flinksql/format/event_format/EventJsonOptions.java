package com.flink.flinksql.format.event_format;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonOptions;

/** Option utils for event-json format. */
public class EventJsonOptions {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> OTHER_FIELD =
            ConfigOptions.key("others")
                    .stringType()
                    .defaultValue("others")
                    .withDescription("扩展字段以json的方式存入该字段");
}