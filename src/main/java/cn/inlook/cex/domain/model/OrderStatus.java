package cn.inlook.cex.domain.model;

// [ZH] 订单生命周期状态
// [EN] Order lifecycle status
public enum OrderStatus {
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELED
}
