package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 订单簿单档位只读视图
// [EN] Read-only view of a single order-book level
@Getter
public class BookLevelView {

    private final long price;
    private final int orderCount;
    private final long totalRemainingQty;

    public BookLevelView(long price, int orderCount, long totalRemainingQty) {
        this.price = price;
        this.orderCount = orderCount;
        this.totalRemainingQty = totalRemainingQty;
    }
}
