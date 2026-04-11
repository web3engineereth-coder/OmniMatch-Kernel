package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 下单或撤单被拒绝时的结构化事件
// [EN] Structured event emitted when an order command is rejected
@Getter
public class OrderRejectedEvent {

    private final String symbol;
    private final long orderId;
    private final long userId;
    private final OrderRejectReason reason;
    private final long timestamp;

    public OrderRejectedEvent(String symbol,
                              long orderId,
                              long userId,
                              OrderRejectReason reason,
                              long timestamp) {
        this.symbol = symbol;
        this.orderId = orderId;
        this.userId = userId;
        this.reason = reason;
        this.timestamp = timestamp;
    }
}
