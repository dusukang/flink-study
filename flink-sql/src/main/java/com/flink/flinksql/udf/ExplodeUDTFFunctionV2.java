package com.flink.flinksql.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("Row<pro_id string,score decimal(10,2)>"))
public class ExplodeUDTFFunctionV2 extends TableFunction<Row> {

    public void eval(String data,String ... fields){
        List<Map> items = JSONObject.parseArray(data, Map.class);
        for (Map item : items) {
            Row row = Row.withNames();
            for (String field : fields) {
                row.setField(field,item.get(field));
            }
            collect(row);
        }
    }

//    @Override
//    public TypeInformation<Row> getResultType() {
//        return Types.ROW(Types.STRING(),Types.DECIMAL());
//    }
}
