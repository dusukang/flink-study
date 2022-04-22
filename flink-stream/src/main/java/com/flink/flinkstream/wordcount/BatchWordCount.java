package com.flink.flinkstream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataSet<String> inputDataSet = env.readTextFile("data/stream/wordcount.txt");

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        DataSet<Tuple2<String, Integer>> flatMapDataSet = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String data, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : data.split(" ")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        flatMapDataSet.groupBy(0).sum(1).print();
    }
}
