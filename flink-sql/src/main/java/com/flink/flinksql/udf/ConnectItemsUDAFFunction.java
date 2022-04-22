package com.flink.flinksql.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConnectItemsUDAFFunction extends AggregateFunction<String, List<String>> {

    private List<String> acc;

    @Override
    public String getValue(List<String> accumulator) {
        return StringUtils.join(accumulator, ",");
    }

    @Override
    public List<String> createAccumulator() {
        return new ArrayList<>();
    }

    public void accumulate(List<String> acc,String input){
        acc.add(input);
    }

    public void retract(List<String> acc,String input){
        acc.remove(input);
    }

    public void merge(List<String> acc,Iterable<List<String>> it){
        Iterator<List<String>> iter = it.iterator();
        while (iter.hasNext()){
            List<String> accOther = iter.next();
            for (String item : accOther) {
                acc.add(item);
            }
        }
    }

    public void resetAccumulator(List<String> acc){
        acc = new ArrayList<>();
    }

}
