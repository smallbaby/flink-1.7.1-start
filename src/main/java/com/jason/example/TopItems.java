package com.jason.example;

import com.jason.example.bean.ItemViewCount;
import com.jason.example.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
// link http://wuchong.me/blog
public class TopItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        // 读本地文件
        URL fileUrl = TopItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<UserBehavior>(filePath, pojoType, fieldOrder);
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
        // eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // timestamp
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.timestamp * 1000;
                    }
                }
        );
        // filter
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        });
        // window watermark
        DataStream<ItemViewCount> windowData = pvData.keyBy("itemId").timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());
        // Top
        DataStream<String> topData = windowData.keyBy("windowEnd").process(new TopNHotItems(3));
        topData.print();
        env.execute();


    }

    static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private int topSize;
        private ListState<ItemViewCount> itemState;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor  =
                    new ListStateDescriptor<ItemViewCount>("itemState-state", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> allItems = new ArrayList<>();
            for(ItemViewCount item: itemState.get()) {
                allItems.add(item);
            }
            itemState.clear();
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            for (ItemViewCount i: allItems) {
                out.collect(i.windowEnd  + "\t " + i.itemId +  "\t" + i.viewCount);
            }
        }
    }

    static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            out.collect(ItemViewCount.of(((org.apache.flink.api.java.tuple.Tuple1<Long>) tuple).f0,
                    window.getEnd(), input.iterator().next()));
        }
    }


    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }


    public static class Item {
        public static long userId;
        public static long itemId;
        public static int categoryId;
        public static String behavior;
        public static long timestamp;
    }
}
