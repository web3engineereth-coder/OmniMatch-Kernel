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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineBatchConsistencyTest {

    @Test
    void shouldMaintainBookConsistencyAcrossBatchOperations() {
        RecordingAccountService accountService = new RecordingAccountService();
        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        for (long orderId = 1; orderId <= 50; orderId++) {
            engine.processOrder(new Order(orderId, 1000L + orderId, OrderSide.BUY, 101L, 1L));
        }
        for (long orderId = 51; orderId <= 100; orderId++) {
            engine.processOrder(new Order(orderId, 1000L + orderId, OrderSide.BUY, 100L, 1L));
        }
        for (long orderId = 101; orderId <= 150; orderId++) {
            engine.processOrder(new Order(orderId, 2000L + orderId, OrderSide.SELL, 105L, 1L));
        }
        for (long orderId = 151; orderId <= 200; orderId++) {
            engine.processOrder(new Order(orderId, 2000L + orderId, OrderSide.SELL, 106L, 1L));
        }

        assertEquals(200, engine.getActiveOrderCount());
        assertEquals(101L, engine.getBestBidPrice());
        assertEquals(105L, engine.getBestAskPrice());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.BUY, 101L).size());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.BUY, 100L).size());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.SELL, 105L).size());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.SELL, 106L).size());

        for (long orderId = 1; orderId <= 10; orderId++) {
            engine.cancelOrder(orderId);
        }
        for (long takerId = 1001; takerId <= 1040; takerId++) {
            engine.processOrder(new Order(takerId, 9000L + takerId, OrderSide.SELL, 101L, 1L));
        }
        for (long takerId = 2001; takerId <= 2025; takerId++) {
            engine.processOrder(new Order(takerId, 9000L + takerId, OrderSide.BUY, 105L, 1L));
        }

        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 101L));
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(105L, engine.getBestAskPrice());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.BUY, 100L).size());
        assertEquals(25, engine.getOrderIdsAtPrice(OrderSide.SELL, 105L).size());
        assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.SELL, 106L).size());
        assertEquals(125, engine.getActiveOrderCount());
        assertEquals(125, countOrdersInBook(engine, List.of(
                new BookLevel(OrderSide.BUY, 101L),
                new BookLevel(OrderSide.BUY, 100L),
                new BookLevel(OrderSide.SELL, 105L),
                new BookLevel(OrderSide.SELL, 106L)
        )));

        for (long orderId = 1; orderId <= 50; orderId++) {
            assertFalse(engine.hasActiveOrder(orderId));
        }
        for (long orderId = 51; orderId <= 100; orderId++) {
            assertTrue(engine.hasActiveOrder(orderId));
        }
        for (long orderId = 101; orderId <= 125; orderId++) {
            assertFalse(engine.hasActiveOrder(orderId));
        }
        for (long orderId = 126; orderId <= 150; orderId++) {
            assertTrue(engine.hasActiveOrder(orderId));
        }
        for (long orderId = 151; orderId <= 200; orderId++) {
            assertTrue(engine.hasActiveOrder(orderId));
        }
    }

    private int countOrdersInBook(MatchingEngine engine, List<BookLevel> levels) {
        int total = 0;
        for (BookLevel level : levels) {
            total += engine.getOrderIdsAtPrice(level.side(), level.price()).size();
        }
        return total;
    }

    private record BookLevel(OrderSide side, long price) {
    }

    private static class RecordingAccountService implements AccountService {
        private final List<TradeEvent> tradeEvents = new ArrayList<>();

        @Override
        public void settleTrade(TradeEvent tradeEvent) {
            tradeEvents.add(tradeEvent);
        }
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> tradeEvents = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            tradeEvents.add(tradeEvent);
        }
    }
}
