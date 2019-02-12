package com.jason.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void simpleCode(DataStream<String> text) {
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split("\\s")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
    }

    public static void main(String[] args) throws Exception {
        // 创建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        // 字符串数据解析成单词和次数 使用Tuple2<String, Integer> 单词/次数(1)
        // flatmap 来解析，因为一行可能包含多个单词
        DataStream<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split("\\s")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 按单词分组（索引为0）  5秒窗口聚合  1号索引字段sum
        DataStream<Tuple2<String, Integer>> windowCounts = wordCounts
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        windowCounts.print().setParallelism(1);
        env.execute("socket window wordcount");
    }
}
