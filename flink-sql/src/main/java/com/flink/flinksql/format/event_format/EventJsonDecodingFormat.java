package com.flink.flinksql.format.event_format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventJsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private List<String> metadataKeys;

    private final String otherField;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    public EventJsonDecodingFormat(boolean ignoreParseErrors, TimestampFormat timestampFormat, String otherField) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.otherField = otherField;
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {
        final List<EventJsonDecodingFormat.ReadableMetadata> readableMetadata =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(EventJsonDecodingFormat.ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .collect(Collectors.toList());
        final List<DataTypes.Field> metadataFields =
                readableMetadata.stream()
                        .map(m -> DataTypes.FIELD(m.key, m.dataType))
                        .collect(Collectors.toList());
        final DataType producedDataType =
                DataTypeUtils.appendRowFields(physicalDataType, metadataFields);
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);
        return new EventJsonDeserializationSchema(producedDataType, producedTypeInfo, ignoreParseErrors, timestampFormat, otherField);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(EventJsonDecodingFormat.ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    /** List of metadata that can be read with this format. */
    enum ReadableMetadata {
        OTHERS(
                "others",
                DataTypes.STRING().nullable(),
                DataTypes.FIELD("others", DataTypes.STRING()));

        final String key;

        final DataType dataType;

        final DataTypes.Field requiredJsonField;

        ReadableMetadata(
                String key,
                DataType dataType,
                DataTypes.Field requiredJsonField) {
            this.key = key;
            this.dataType = dataType;
            this.requiredJsonField = requiredJsonField;
        }
    }
}


