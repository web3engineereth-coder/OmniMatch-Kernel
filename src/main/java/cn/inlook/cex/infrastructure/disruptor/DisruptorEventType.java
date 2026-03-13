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
    CANCEL_ORDER,

    // ==========================================
    // [ZH] 系统控制级指令
    // [EN] System Control Level Commands
    // ==========================================

    // [ZH] 触发全量内存快照 (保障无锁一致性)
    // [EN] Trigger full memory snapshot (Ensures lock-free consistency)
    MAKE_SNAPSHOT
}