package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.OrderCanceledEvent;

// [ZH] 撤单事件发布边界
// [EN] Publication boundary for cancel events
public interface OrderCanceledEventPublisher {

    void publish(OrderCanceledEvent event);
}
