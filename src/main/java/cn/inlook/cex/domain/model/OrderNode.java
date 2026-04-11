package cn.inlook.cex.domain.model;

import lombok.Getter;
import lombok.Setter;

// [ZH] 订单簿中的挂单节点，承载 O(1) 撤单所需的链表元数据
// [EN] Resting-order node carrying linked-list metadata for O(1) cancellation
@Getter
@Setter
public class OrderNode {

    private final Order order;
    private OrderNode prev;
    private OrderNode next;
    private PriceLevel priceLevel;

    public OrderNode(Order order) {
        this.order = order;
    }

    public long getOrderId() {
        return order.getOrderId();
    }

    public OrderSide getSide() {
        return order.getSide();
    }

    public long getPrice() {
        return order.getPrice();
    }

    public long getOriginalQty() {
        return order.getOriginalAmount();
    }

    public long getRemainingQty() {
        return order.getRemainingAmount();
    }

    public OrderStatus getStatus() {
        return order.getStatus();
    }

    public boolean isFilled() {
        return order.isFilled();
    }

    public boolean isCanceled() {
        return order.isCanceled();
    }

    public void fill(long tradedQty) {
        order.fill(tradedQty);
    }

    public void cancel() {
        order.cancel();
    }
}
