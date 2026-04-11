package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.OrderRejectedEvent;

// [ZH] 默认空实现，避免在未接线时引入额外副作用
// [EN] Default no-op implementation to avoid extra side effects when not wired
public class NoopOrderRejectedEventPublisher implements OrderRejectedEventPublisher {

    @Override
    public void publish(OrderRejectedEvent event) {
        // Intentionally no-op.
    }
}
