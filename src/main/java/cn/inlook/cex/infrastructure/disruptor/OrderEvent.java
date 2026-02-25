package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.model.Order;
import com.lmax.disruptor.EventFactory;
import lombok.Getter;
import lombok.Setter;

// [ZH] Disruptor 环形队列中的事件载体
// [EN] Event carrier in the Disruptor RingBuffer
@Getter
@Setter
public class OrderEvent {

    // [ZH] 实际要处理的订单
    // [EN] The actual order to be processed
    private Order order;

    // [ZH] 事件工厂，Disruptor 启动时会预先实例化填满 RingBuffer，实现 Zero-GC
    // [EN] Event factory, Disruptor pre-allocates these in RingBuffer at startup for Zero-GC
    public static final EventFactory<OrderEvent> FACTORY = new EventFactory<OrderEvent>() {
        @Override
        public OrderEvent newInstance() {
            return new OrderEvent();
        }
    };
}