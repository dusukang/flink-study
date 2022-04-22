package com.flink.flinksql.udf;

import org.apache.flink.table.functions.TableFunction;

public class ExplodeUDTFFunction extends TableFunction<String> {

    public void eval(String input,String delimiter){
        String[] items = input.split(delimiter);
        for (String item : items) {
            collect(item);
        }
    }

}
