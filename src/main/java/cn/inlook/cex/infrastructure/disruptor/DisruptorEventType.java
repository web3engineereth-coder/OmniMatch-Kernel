package cn.inlook.cex.infrastructure.disruptor;

import lombok.Getter;

// [ZH] Disruptor 环形队列事件类型
// [EN] Disruptor RingBuffer Event Type
@Getter
public enum DisruptorEventType {
    // [ZH] 下达新订单
    // [EN] Place new order
    PLACE_ORDER,

    // [ZH] 撤销现有订单
    // [EN] Cancel existing order
    CANCEL_ORDER
}