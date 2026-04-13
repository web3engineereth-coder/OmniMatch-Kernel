package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 撤单成功后的结构化事件
// [EN] Structured event emitted when an order is canceled
@Getter
public class OrderCanceledEvent implements EngineEvent {

    private final String symbol;
    private final long orderId;
    private final long userId;
    private final long remainingQuantity;
    private final long timestamp;

    public OrderCanceledEvent(String symbol,
                              long orderId,
                              long userId,
                              long remainingQuantity,
                              long timestamp) {
        this.symbol = symbol;
        this.orderId = orderId;
        this.userId = userId;
        this.remainingQuantity = remainingQuantity;
        this.timestamp = timestamp;
    }

    @Override
    public EngineEventType getEventType() {
        return EngineEventType.ORDER_CANCELED;
    }
}
