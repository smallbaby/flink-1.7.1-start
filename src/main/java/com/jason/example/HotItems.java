package com.jason.example;


import com.jason.example.bean.ItemViewCount;
import com.jason.example.bean.UserBehavior;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 每隔5分钟输出过去一小时内点击量最多的前 N 个商品
 * 数据格式:用户ID、商品ID、商品类目ID、行为类型和时间戳
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1); // 全局并发为1
        // input
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<UserBehavior>(filePath, pojoType, fieldOrder);
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
        // 统计过去5分钟热门商品， 5分钟可以为processtime，也可以是eventtime
        // 1 告诉flink我们要按照eventtime来计算
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2 指定如何获取业务时间以及实现watermark，本地数据无乱序使用 AscendingTimestampExtractor，
        //   真实业务场景存在乱序，使用 BoundedOutOfOrdernessTimestampExtractor
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.timestamp * 1000;
                    }
                }
        ); // 返回带有时间标记的数据流
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.behavior.equals("pv");
            }
        }); // 过滤，只要点击类型的数据

        // sliding window 窗口操作，5分钟统计一次最近一小时商品的点击量，窗口大小为1小时，5分钟滑动一次，
        // [00:00,01:00],[00:05,01:05],[00:10,01:10]

        DataStream<ItemViewCount> windowData = pvData
                .keyBy("itemId")  // 分组
                .timeWindow(Time.minutes(60), Time.minutes(5)) // timeWindow(Time size, Time slide) 滑动
                .aggregate(new CountAgg(), new WindowResultFunction()); // 增量聚合  CountAgg：遇到一条就 + 1

        // 按窗口end再分组，求TOP
        DataStream<String> topItems = windowData.keyBy("windowEnd").process(new TopNHotItems(3));
        topItems.print();
        env.execute();

    }

    // 求某个窗口前N名热门商品， 窗口时间戳，输出字符串
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private final int topSize;
        // 用于存储商品和点击的状态，收集完同一个窗口的数据后，再触发TopN计算
        private ListState<ItemViewCount> itemState;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态注册
            ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new ListStateDescriptor<ItemViewCount>(
                    "itemState-state", ItemViewCount.class
            );
            itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
        }

        // 收到一条数据，就注册一个windowEnd+1的定时器
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取所有商品的点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }

            // 清理状态中的数据，释放空间
            itemState.clear();
            // 点击量排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 格式化输出
            StringBuffer result = new StringBuffer();
            result.append("===================================\n");
            result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                result.append("No").append(i).append(":")
                        .append(" 商品ID=").append(currentItem.itemId)
                        .append(" 浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("===========================\n\n");
            out.collect(result.toString());
        }
    }
}


