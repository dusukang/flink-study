package com.flink.flinksql.format.event_format;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventJsonDeserializationSchema implements DeserializationSchema<RowData> {

    private TypeInformation<RowData> resultTypeInfo;
    private List<RowType.RowField> rowTypeFields;
    private String otherField;

    public EventJsonDeserializationSchema(DataType dataType, TypeInformation<RowData> resultTypeInfo, boolean ignoreParseErrors,
                                          TimestampFormat timestampFormatOption, String otherField) {
        this.resultTypeInfo = resultTypeInfo;
        final RowType rowType = (RowType) dataType.getLogicalType();
        this.rowTypeFields = rowType.getFields();
        this.otherField = otherField;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        String line = new String(message);

        final JSONObject jsonObject = JSONObject.parseObject(line);
        GenericRowData rowData = new GenericRowData(rowTypeFields.size() );
        JSONObject others = new JSONObject();
        List<String> existField = new ArrayList<>();
        for (int i=0; i<rowTypeFields.size(); i++) {
            final RowType.RowField rowField = rowTypeFields.get(i);
            if (jsonObject.containsKey(rowField.getName())) {
                existField.add(rowField.getName());
                rowData.setField(i, new BinaryStringData(jsonObject.getString(rowField.getName())));
            }
        }
        for (String key : jsonObject.keySet()) {
            if (!existField.contains(key)) {
                others.put(key, jsonObject.get(key));
            }
        }
        rowData.setField(rowTypeFields.size() - 1, new BinaryStringData(others.toJSONString()));
        out.collect(rowData);
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
}
