package com.flink.flinksql.flinkcdc;

import com.flink.flinksql.enums.TableRowTypeInfoEnum;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


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
        String[] fileds = TableRowTypeInfoEnum.tableRowTyeInfoMap.get(table).f0;
        if(op.equals("r") || op.equals("c")){
            Row row = Row.withPositions(RowKind.INSERT,fileds.length);
            for (int i = 0; i < fileds.length; i++) {
                row.setField(i,after.get(fileds[i]));
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("d")){
            Row row = Row.withPositions(RowKind.DELETE,fileds.length);
            for (int i = 0; i < fileds.length; i++) {
                row.setField(i,before.get(fileds[i]));
            }
            collector.collect(Tuple2.of(table,row));
        }else if(op.equals("u")){
            Row rowBefore = Row.withPositions(RowKind.UPDATE_BEFORE,fileds.length);
            Row rowAfter = Row.withPositions(RowKind.UPDATE_AFTER,fileds.length);
            for (int i = 0; i < fileds.length; i++) {
                rowBefore.setField(i,before.get(fileds[i]));
                rowAfter.setField(i,after.get(fileds[i]));
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
