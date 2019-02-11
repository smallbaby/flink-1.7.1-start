package com.jason.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;


class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] tokens = s.toLowerCase().split("\\W+");
        for (String token : tokens) {
            if (token.length() > 0) {
                collector.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}


public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        DataSet<String> text = env.readTextFile(args[0]);
        DataSet<Tuple2<String, Integer>> counts
                = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        //  socket
        DataStream<String> tt = StreamExecutionEnvironment.createLocalEnvironment().socketTextStream("", 0000);
        // 解析   每行调用map，转为tuple<String,Long>类型，tuple[0] 代表具体的数据，tuple[1] 是eventtime
        DataStream<Tuple2<String, Long>> inputMap = tt.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });
        // 抽取时间，生成watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L; // 10秒
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            // 定义如何提取timestamp
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1; // 数据中的timestamp,跟当前max比较
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("key:" + element.f0);
                System.out.println("event_time:[" + element.f1 + "|" + sdf.format(element.f1));
                System.out.println("currentMaxTimestamp:" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp));
                System.out.println("watermark:" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()));
                return timestamp;
            }
        });

        // window触发条件:
        // 1 watermark时间 >= window_end_time
        // 2 在window_start_time ~ end_time 中有数据存在，左闭右开
        // group  aggr 窗口3秒，输出<key, 元素个数, 窗口最早元素时间，最后元素时间, 窗口开始时间,结束时间>
        DataStream<String> window = waterMarkStream.keyBy(0).window(
                TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(2)) // 允许数据迟到2秒
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            // 对window内的数据排序，保证数据的顺序
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                String key = tuple.toString();
                List<Long> arrarList = new ArrayList<>();
                Iterator<Tuple2<String, Long>> it = input.iterator();
                while (it.hasNext()) {
                    Tuple2<String, Long> next = it.next();
                    arrarList.add(next.f1);
                }
                Collections.sort(arrarList);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0) + "-" + sdf.format(arrarList.get(arrarList.size() - 1))) + "," + window.getStart() + "-" + window.getEnd();
                out.collect(result);
            }
        });
        // 延迟到达的数据
        // 1、flink默认为丢弃
        // 2、设置允许数据延迟时间  allowedLateness
        // 3、收集迟到的数据 sideOutputLateData

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        SingleOutputStreamOperator<String> late_window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3))).sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    // 窗口内排序
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0) + "-" + sdf.format(arrarList.get(arrarList.size() - 1))) + "," + window.getStart() + "-" + window.getEnd();
                        out.collect(result);
                    }
                });

        // 迟到的数据，可打印、保存
        DataStream<Tuple2<String, Long>> sideOutput = late_window.getSideOutput(outputTag);
        sideOutput.print();


        counts.writeAsCsv(args[1]);
        env.execute();
    }
}
