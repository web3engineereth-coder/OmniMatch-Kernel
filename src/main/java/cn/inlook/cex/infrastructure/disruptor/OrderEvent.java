package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.model.Order;
import com.lmax.disruptor.EventFactory;
import lombok.Getter;
import lombok.Setter;

// [ZH] Disruptor 环形队列中的多功能事件载体 (采用独立枚举路由)
// [EN] Multi-purpose event carrier in the Disruptor RingBuffer (Using standalone enum routing)
@Getter
@Setter
public class OrderEvent {

    // [ZH] 引入你定义的独立枚举作为路由标识
    // [EN] Import your standalone enum as the routing flag
    private DisruptorEventType eventType;

    // [ZH] 针对 PLACE_ORDER：实际要处理的新订单
    // [EN] For PLACE_ORDER: The actual new order to be processed
    private Order order;

    // [ZH] 针对 CANCEL_ORDER：需要被 O(1) 极限撤销的订单 ID
    // [EN] For CANCEL_ORDER: The target order ID to be canceled via O(1)
    private long cancelOrderId;

    // [ZH] 清空引用，防止内存泄漏与脏数据污染
    // [EN] Clear references to prevent memory leaks and dirty data pollution
    public void clear() {
        this.eventType = null;
        this.order = null;
        this.cancelOrderId = 0L;
    }

    // [ZH] 事件工厂，实现 Zero-GC
    // [EN] Event factory for Zero-GC
    public static final EventFactory<OrderEvent> FACTORY = new EventFactory<OrderEvent>() {
        @Override
        public OrderEvent newInstance() {
            return new OrderEvent();
        }
    };
}