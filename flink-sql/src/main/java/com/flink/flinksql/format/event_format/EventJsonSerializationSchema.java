package com.flink.flinksql.format.event_format;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import java.util.List;

public class EventJsonSerializationSchema implements SerializationSchema<RowData> {

    private List<RowType.RowField> rowTypeFields;
    private String othersField;

    public EventJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat, String otherField) {
        this.rowTypeFields = rowType.getFields();
        this.othersField = otherField;
    }

    @Override
    public byte[] serialize(RowData rowData) {
        JSONObject jsonObject = new JSONObject();
        for (int i=0; i< rowTypeFields.size(); i++) {
            final RowType.RowField rowField = rowTypeFields.get(i);
            final String rowLine = rowData.getString(i).toString();
            if (othersField.equals(rowField.getName())) {
                jsonObject.putAll(JSONObject.parseObject(rowLine));
            } else {
                jsonObject.put(rowField.getName(), rowLine);
            }
        }
        return jsonObject.toJSONString().getBytes();
    }
}