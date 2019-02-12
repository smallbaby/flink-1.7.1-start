package com.jason.example.bean;

public class ItemViewCount {
    public long itemId;
    public long windowEnd; // 窗口结束时间戳
    public long viewCount;

    public static ItemViewCount of(Long itemId, Long windowEnd, Long viewCount) {
        ItemViewCount itemViewCount = new ItemViewCount();
        itemViewCount.itemId = itemId;
        itemViewCount.windowEnd = windowEnd;
        itemViewCount.viewCount = viewCount;
        return itemViewCount;
    }
}
