package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 活跃订单的只读查询视图
// [EN] Read-only query view of an active order
@Getter
public class OrderView {

    private final long orderId;
    private final long userId;
    private final String symbol;
    private final OrderSide side;
    private final long price;
    private final long originalQty;
    private final long remainingQty;
    private final OrderStatus status;
    private final OrderRejectReason rejectReason;

    public OrderView(long orderId,
                     long userId,
                     String symbol,
                     OrderSide side,
                     long price,
                     long originalQty,
                     long remainingQty,
                     OrderStatus status) {
        this(orderId, userId, symbol, side, price, originalQty, remainingQty, status, null);
    }

    public OrderView(long orderId,
                     long userId,
                     String symbol,
                     OrderSide side,
                     long price,
                     long originalQty,
                     long remainingQty,
                     OrderStatus status,
                     OrderRejectReason rejectReason) {
        this.orderId = orderId;
        this.userId = userId;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.originalQty = originalQty;
        this.remainingQty = remainingQty;
        this.status = status;
        this.rejectReason = rejectReason;
    }
}
