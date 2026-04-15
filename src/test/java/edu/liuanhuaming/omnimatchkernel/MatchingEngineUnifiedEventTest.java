package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.EngineEvent;
import cn.inlook.cex.domain.model.EngineEventType;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.EngineEventPublisher;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineUnifiedEventTest {

    @Test
    void shouldPublishTradeCancelAndRejectThroughUnifiedEventPublisher() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);
        accountService.createAccount(1003L, 100L, 0L);

        RecordingEngineEventPublisher publisher = new RecordingEngineEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        engine.processOrder(new Order(1L, 1001L, "BTC-USDT", OrderSide.SELL, 100L, 10L));
        engine.processOrder(new Order(2L, 1002L, "BTC-USDT", OrderSide.BUY, 100L, 5L));
        engine.cancelOrder(1L);
        engine.processOrder(new Order(3L, 1003L, "BTC-USDT", OrderSide.BUY, 100L, 2L));

        assertEquals(3, publisher.events.size());

        EngineEvent tradeEvent = publisher.events.get(0);
        assertEquals(EngineEventType.TRADE, tradeEvent.getEventType());
        assertEquals("BTC-USDT", tradeEvent.getSymbol());
        assertTrue(tradeEvent.getTimestamp() > 0);
        TradeEvent trade = assertInstanceOf(TradeEvent.class, tradeEvent);
        assertEquals(1L, trade.getMakerOrderId());
        assertEquals(2L, trade.getTakerOrderId());

        EngineEvent canceledEvent = publisher.events.get(1);
        assertEquals(EngineEventType.ORDER_CANCELED, canceledEvent.getEventType());
        assertEquals("BTC-USDT", canceledEvent.getSymbol());
        OrderCanceledEvent canceled = assertInstanceOf(OrderCanceledEvent.class, canceledEvent);
        assertEquals(1L, canceled.getOrderId());
        assertEquals(5L, canceled.getRemainingQuantity());

        EngineEvent rejectedEvent = publisher.events.get(2);
        assertEquals(EngineEventType.ORDER_REJECTED, rejectedEvent.getEventType());
        assertEquals("BTC-USDT", rejectedEvent.getSymbol());
        OrderRejectedEvent rejected = assertInstanceOf(OrderRejectedEvent.class, rejectedEvent);
        assertEquals(3L, rejected.getOrderId());
        assertEquals(OrderRejectReason.INSUFFICIENT_BALANCE, rejected.getReason());
    }

    private static class RecordingEngineEventPublisher implements EngineEventPublisher {
        private final List<EngineEvent> events = new ArrayList<>();

        @Override
        public void publish(EngineEvent event) {
            events.add(event);
        }
    }
}
