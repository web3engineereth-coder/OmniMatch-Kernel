package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.OrderRejectedEvent;
import lombok.extern.slf4j.Slf4j;

// [ZH] 承接拒单日志的最小发布器
// [EN] Minimal publisher carrying rejection logging out of the core path
@Slf4j
public class LoggingOrderRejectedEventPublisher implements OrderRejectedEventPublisher {

    @Override
    public void publish(OrderRejectedEvent event) {
        log.warn("ORDER_REJECTED: symbol={} orderId={} userId={} reason={}",
                event.getSymbol(),
                event.getOrderId(),
                event.getUserId(),
                event.getReason());
    }
}
