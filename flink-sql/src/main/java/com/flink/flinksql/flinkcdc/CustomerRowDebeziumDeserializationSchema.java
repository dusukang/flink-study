package com.flink.flinksql.flinkcdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerRowDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Tuple2<String,Row>> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String,Row>> collector) throws Exception {
        Struct struct = (Struct)sourceRecord.value();
        Struct sourceStruct = struct.getStruct("source");
//        String db = sourceStruct.getString("db");
        String table = sourceStruct.getString("table");
        Struct before = struct.getStruct("before");
        Struct after = struct.getStruct("after");
        String op = struct.getString("op");
        if(op.equals("r") || op.equals("c")){
            Row row = Row.withNames(RowKind.INSERT);
            List<Field> fields = after.schema().fields();
            for (Field field : fields) {
                row.setField(field.name(),after.get(field));
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("d")){
            Row row = Row.withNames(RowKind.DELETE);
            List<Field> fields = before.schema().fields();
            for (Field field : fields) {
                row.setField(field.name(),before.get(field));
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("u")){
            Row rowBefore = Row.withNames(RowKind.UPDATE_BEFORE);
            Row rowAfter = Row.withNames(RowKind.UPDATE_AFTER);
            List<Field> fields = before.schema().fields();
            for (Field field : fields) {
                rowBefore.setField(field.name(),before.get(field));
                rowAfter.setField(field.name(),after.get(field));
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

}
