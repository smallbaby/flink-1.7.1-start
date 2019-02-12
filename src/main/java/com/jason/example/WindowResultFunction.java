package com.jason.example;

import com.jason.example.bean.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, //  窗口主键
                      TimeWindow window, // 窗口
                      Iterable<Long> input, // 聚合结果，即count
                      Collector<ItemViewCount> collector // 输出类型，ItemViewCount
    ) throws Exception {
        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long count = input.iterator().next();
        collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}
