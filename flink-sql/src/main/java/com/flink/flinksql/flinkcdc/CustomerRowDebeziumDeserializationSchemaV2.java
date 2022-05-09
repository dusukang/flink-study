package com.flink.flinksql.flinkcdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class CustomerRowDebeziumDeserializationSchemaV2 implements DebeziumDeserializationSchema<Tuple2<String,Row>> {

    private Map<String, DataType[]> tableDataTypesMap;

    public CustomerRowDebeziumDeserializationSchemaV2(Map<String, DataType[]> tableDataTypesMap) {
        this.tableDataTypesMap = tableDataTypesMap;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String,Row>> collector) throws Exception {
        Struct struct = (Struct)sourceRecord.value();
        Struct sourceStruct = struct.getStruct("source");
//        String db = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");
        Struct before = struct.getStruct("before");
        Struct after = struct.getStruct("after");
        String op = struct.getString("op");
        DataType[] dataTypes = tableDataTypesMap.get(table);
        if(op.equals("r") || op.equals("c")){
            List<Field> fields = after.schema().fields();
            Row row = Row.withPositions(RowKind.INSERT,fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Object formatVal = fieldValFormat(dataTypes[i].toString(), after.get(fields.get(i)));
                row.setField(i,formatVal);
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("d")){
            List<Field> fields = before.schema().fields();
            Row row = Row.withPositions(RowKind.DELETE,fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Object formatVal = fieldValFormat(dataTypes[i].toString(), before.get(fields.get(i)));
                row.setField(i,formatVal);
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("u")){
            List<Field> fields = before.schema().fields();
            Row rowBefore = Row.withPositions(RowKind.UPDATE_BEFORE,fields.size());
            Row rowAfter = Row.withPositions(RowKind.UPDATE_AFTER,fields.size());
            for (int i = 0; i < fields.size(); i++) {
                Object beforeFormatVal = fieldValFormat(dataTypes[i].toString(), before.get(fields.get(i)));
                Object afterFormatVal = fieldValFormat(dataTypes[i].toString(), after.get(fields.get(i)));
                rowBefore.setField(i,beforeFormatVal);
                rowAfter.setField(i,afterFormatVal);
            }
            collector.collect(Tuple2.of(table,rowBefore));
            collector.collect(Tuple2.of(table,rowAfter));
        }
    }

    @Override
    public TypeInformation<Tuple2<String,Row>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String,Row>>() {
            @Override
            public TypeInformation<Tuple2<String,Row>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    public Object fieldValFormat(String dataType,Object val) throws InterruptedException {
        if(dataType.equals("FLOAT") && val instanceof String){
            return Float.valueOf((String)val);
        }
        if(dataType.startsWith("DECIMAL") && val instanceof String ){
            return new BigDecimal((String)val);
        }
        if(dataType.startsWith("TIME(") && val instanceof String ){
            return LocalTime.parse((String)val,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        if(dataType.startsWith("TIMESTAMP(") && val instanceof String ){
            return LocalDateTime.parse((String)val,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        return val;
    }
}
