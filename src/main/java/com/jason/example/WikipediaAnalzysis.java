package com.jason.example;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalzysis {
    public static void main(String[] args) throws Exception {
        // 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置time 默认为processtime ，设置为event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置并行度 默认为cpu个数
        env.setParallelism(1);

        // stream source
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());
        // 类分组
        KeyedStream<WikipediaEditEvent, String> keyedEdits =
                edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                        return wikipediaEditEvent.getUser();
                    }
                });

        // 聚合
        DataStream<Tuple2<String, Long>> result = keyedEdits
                // 滚动时间窗口
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L),
                        // tuple2中的数据是指每5秒钟针对每个用户统计的字节变化量
                        new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                                acc.f0 = event.getUser();
                                acc.f1 += event.getByteDiff();
                                return acc;
                            }
                        });
        result.print(); // 打印到屏幕

        // 写入kafka
        result.map(new MapFunction<Tuple2<String, Long>, String>() {
            // 将<string long> 的stream转化成string方法写入kafka
            @Override
            public String map(Tuple2<String, Long> tuple) throws Exception {
                return tuple.toString();
            }
        }).addSink(new FlinkKafkaProducer08<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));
        env.execute();
    }
}
