package com.flink.flinkstream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStreamSource<String> dataStream = env.readTextFile("data/stream/wordcount.txt");

        dataStream.print();

        env.execute("file source");
    }
}
