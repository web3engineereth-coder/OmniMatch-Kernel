package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 标准化撤单命令，作为撮合引擎的输入边界
// [EN] Standardized cancel-order command for the matching engine input boundary
@Getter
public class CancelOrderCommand {

    private final String symbol;
    private final long orderId;
    private final long userId;
    private final long timestamp;

    public CancelOrderCommand(long orderId, long userId, long timestamp) {
        this(Order.DEFAULT_SYMBOL, orderId, userId, timestamp);
    }

    public CancelOrderCommand(String symbol, long orderId, long userId, long timestamp) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol must not be blank.");
        }
        this.symbol = symbol;
        this.orderId = orderId;
        this.userId = userId;
        this.timestamp = timestamp;
    }
}
