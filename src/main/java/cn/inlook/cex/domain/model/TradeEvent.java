package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 撮合核心输出的结构化成交结果
// [EN] Structured execution result emitted by the matching core
@Getter
public class TradeEvent {

    private final long makerOrderId;
    private final long takerOrderId;
    private final long buyerId;
    private final long sellerId;
    private final long price;
    private final long quantity;
    private final long makerRemainingQty;
    private final long takerRemainingQty;
    private final OrderStatus makerStatus;
    private final OrderStatus takerStatus;

    public TradeEvent(long makerOrderId,
                      long takerOrderId,
                      long buyerId,
                      long sellerId,
                      long price,
                      long quantity,
                      long makerRemainingQty,
                      long takerRemainingQty,
                      OrderStatus makerStatus,
                      OrderStatus takerStatus) {
        this.makerOrderId = makerOrderId;
        this.takerOrderId = takerOrderId;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.price = price;
        this.quantity = quantity;
        this.makerRemainingQty = makerRemainingQty;
        this.takerRemainingQty = takerRemainingQty;
        this.makerStatus = makerStatus;
        this.takerStatus = takerStatus;
    }
}
