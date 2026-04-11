package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.AccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MatchingEngineTradeEventTest {

    static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> events = new ArrayList<>();

        @Override
        public void publish(TradeEvent event) {
            events.add(event);
        }

        public List<TradeEvent> getEvents() {
            return events;
        }
    }

    static class StubAccountService implements AccountService {
        @Override
        public boolean reserveForOrder(Order order) {
            return true;
        }

        @Override
        public void releaseOnCancel(Order order) {
        }

        @Override
        public void settleTrade(TradeEvent tradeEvent) {
        }
    }

    @Test
    void shouldPublishTradeEvent_whenMatchOccurs() {
        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(new StubAccountService(), publisher);

        Order ask = new Order(1L, 1001L, OrderSide.SELL, 100L, 10L);
        Order bid = new Order(2L, 1002L, OrderSide.BUY, 100L, 10L);

        engine.processOrder(ask);
        engine.processOrder(bid);

        assertEquals(1, publisher.getEvents().size(), "A trade event should be published");

        TradeEvent event = publisher.getEvents().get(0);
        assertEquals(1L, event.getMakerOrderId());
        assertEquals(2L, event.getTakerOrderId());
        assertEquals(1002L, event.getBuyerId());
        assertEquals(1001L, event.getSellerId());
        assertEquals(100L, event.getPrice());
        assertEquals(10L, event.getQuantity());
        assertEquals(0L, event.getMakerRemainingQty());
        assertEquals(0L, event.getTakerRemainingQty());
    }

    @Test
    void shouldNotPublishTradeEvent_whenNoMatchOccurs() {
        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(new StubAccountService(), publisher);

        Order ask = new Order(1L, 1001L, OrderSide.SELL, 105L, 10L);
        Order bid = new Order(2L, 1002L, OrderSide.BUY, 100L, 10L);

        engine.processOrder(ask);
        engine.processOrder(bid);

        assertEquals(0, publisher.getEvents().size(), "No trade event should be published");
    }
}