package com.flink.flinksql.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MiddleValueUDAFFunction extends AggregateFunction<Double, List<Double>> {

    private List<Double> acc;

    @Override
    public Double getValue(List<Double> acc) {
        if(acc.size()==0){
            return null;
        }
        acc.sort((val1,val2)->val1>=val2 ? 1:-1);
        return acc.get(acc.size()/2);
    }

    @Override
    public List<Double> createAccumulator() {
        return new ArrayList<>();
    }

    public void accumulate(List<Double> acc,Double input){
        acc.add(input);
    }

    public void retract(List<Double> acc,Double input){
        acc.remove(input);
    }

    public void merge(List<Double> acc,Iterable<List<Double>> it){
        Iterator<List<Double>> iter = it.iterator();
        while (iter.hasNext()){
            List<Double> accOther = iter.next();
            for (Double item : accOther) {
                acc.add(item);
            }
        }
    }

    public void resetAccumulator(List<Double> acc){
        acc = new ArrayList<>();
    }
}
