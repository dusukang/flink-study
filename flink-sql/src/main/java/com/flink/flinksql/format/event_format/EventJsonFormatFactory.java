package com.flink.flinksql.format.event_format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class EventJsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "event-json";

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);
        final String others = formatOptions.getOptional(EventJsonOptions.OTHER_FIELD).orElse(null);
        return new EventJsonDecodingFormat(ignoreParseErrors, timestampFormat, others);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        TimestampFormat timestampFormat = JsonOptions.getTimestampFormat(formatOptions);
        final String others = formatOptions.getOptional(EventJsonOptions.OTHER_FIELD).orElse(null);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }

            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new EventJsonSerializationSchema(rowType, timestampFormat, others);
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(EventJsonOptions.OTHER_FIELD);
        return options;
    }
}
