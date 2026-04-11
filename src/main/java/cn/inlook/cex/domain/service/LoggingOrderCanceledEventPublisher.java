package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.OrderCanceledEvent;
import lombok.extern.slf4j.Slf4j;

// [ZH] 承接撤单日志的最小发布器
// [EN] Minimal publisher carrying cancel logging out of the core path
@Slf4j
public class LoggingOrderCanceledEventPublisher implements OrderCanceledEventPublisher {

    @Override
    public void publish(OrderCanceledEvent event) {
        log.info("ORDER_CANCELED: symbol={} orderId={} userId={} remaining={}",
                event.getSymbol(),
                event.getOrderId(),
                event.getUserId(),
                event.getRemainingQuantity());
    }
}
