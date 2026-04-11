package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.OrderRejectedEvent;

// [ZH] 拒单事件发布边界
// [EN] Publication boundary for rejection events
public interface OrderRejectedEventPublisher {

    void publish(OrderRejectedEvent event);
}
