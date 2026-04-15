package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.EngineEvent;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.TradeEvent;

// [ZH] 统一事件分发适配器：内部统一发布，对外复用现有细分 publisher
// [EN] Unified event dispatch adapter: publish once internally while reusing existing specialized publishers
public class DispatchingEngineEventPublisher implements EngineEventPublisher {

    private final TradeEventPublisher tradeEventPublisher;
    private final OrderCanceledEventPublisher orderCanceledEventPublisher;
    private final OrderRejectedEventPublisher orderRejectedEventPublisher;

    public DispatchingEngineEventPublisher(TradeEventPublisher tradeEventPublisher,
                                           OrderCanceledEventPublisher orderCanceledEventPublisher,
                                           OrderRejectedEventPublisher orderRejectedEventPublisher) {
        this.tradeEventPublisher = tradeEventPublisher;
        this.orderCanceledEventPublisher = orderCanceledEventPublisher;
        this.orderRejectedEventPublisher = orderRejectedEventPublisher;
    }

    @Override
    public void publish(EngineEvent event) {
        if (event instanceof TradeEvent tradeEvent) {
            tradeEventPublisher.publish(tradeEvent);
            return;
        }
        if (event instanceof OrderCanceledEvent orderCanceledEvent) {
            orderCanceledEventPublisher.publish(orderCanceledEvent);
            return;
        }
        if (event instanceof OrderRejectedEvent orderRejectedEvent) {
            orderRejectedEventPublisher.publish(orderRejectedEvent);
            return;
        }
        throw new IllegalArgumentException("Unsupported engine event type: " + event.getClass().getName());
    }
}
