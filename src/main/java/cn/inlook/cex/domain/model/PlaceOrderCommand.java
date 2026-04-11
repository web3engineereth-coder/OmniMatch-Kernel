package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 标准化下单命令，作为撮合引擎的输入边界
// [EN] Standardized place-order command for the matching engine input boundary
@Getter
public class PlaceOrderCommand {

    private final long orderId;
    private final long userId;
    private final String symbol;
    private final OrderSide side;
    private final long price;
    private final long quantity;
    private final long timestamp;

    public PlaceOrderCommand(long orderId,
                             long userId,
                             OrderSide side,
                             long price,
                             long quantity,
                             long timestamp) {
        this(orderId, userId, Order.DEFAULT_SYMBOL, side, price, quantity, timestamp);
    }

    public PlaceOrderCommand(long orderId,
                             long userId,
                             String symbol,
                             OrderSide side,
                             long price,
                             long quantity,
                             long timestamp) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol must not be blank.");
        }
        if (side == null) {
            throw new IllegalArgumentException("Side must not be null.");
        }
        if (price <= 0 || quantity <= 0) {
            throw new IllegalArgumentException("Price and quantity must be strictly positive.");
        }
        this.orderId = orderId;
        this.userId = userId;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.timestamp = timestamp;
    }
}
