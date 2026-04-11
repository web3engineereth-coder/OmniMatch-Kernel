package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
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

class MatchingEngineInvariantTest {

    private final RecordingAccountService accountService = new RecordingAccountService();
    private final RecordingTradeEventPublisher tradeEventPublisher = new RecordingTradeEventPublisher();
    private final MatchingEngine engine = new MatchingEngine(accountService, tradeEventPublisher);

    @Test
    void shouldMaintainBestPriceCorrectness() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 99L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(3L, 103L, OrderSide.BUY, 100L, 5L));
        engine.processOrder(new Order(4L, 201L, OrderSide.SELL, 105L, 5L));
        engine.processOrder(new Order(5L, 202L, OrderSide.SELL, 103L, 5L));
        engine.processOrder(new Order(6L, 203L, OrderSide.SELL, 104L, 5L));

        assertEquals(101L, engine.getBestBidPrice());
        assertEquals(103L, engine.getBestAskPrice());
    }

    @Test
    void shouldKeepOrderMapAndOrderBookConsistentWithoutDanglingNodes() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 100L, 5L));
        engine.processOrder(new Order(3L, 201L, OrderSide.SELL, 105L, 5L));
        engine.processOrder(new Order(4L, 202L, OrderSide.SELL, 106L, 5L));
        engine.cancelOrder(2L);

        assertTrue(engine.hasActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(2L));
        assertTrue(engine.hasActiveOrder(3L));
        assertTrue(engine.hasActiveOrder(4L));

        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.BUY, 101L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(List.of(3L), engine.getOrderIdsAtPrice(OrderSide.SELL, 105L));
        assertEquals(List.of(4L), engine.getOrderIdsAtPrice(OrderSide.SELL, 106L));

        assertEquals(3, countOrdersInMap());
        assertEquals(3, countOrdersInBook(
                new BookLevel(OrderSide.BUY, 101L),
                new BookLevel(OrderSide.BUY, 100L),
                new BookLevel(OrderSide.SELL, 105L),
                new BookLevel(OrderSide.SELL, 106L)
        ));
    }

    @Test
    void shouldEnforceOrderLifecycleInBookPresence() {
        engine.processOrder(new Order(1L, 201L, OrderSide.SELL, 100L, 10L));
        engine.processOrder(new Order(2L, 101L, OrderSide.BUY, 100L, 5L));

        Order partiallyFilledMaker = engine.findActiveOrder(1L);
        assertEquals(5L, partiallyFilledMaker.getRemainingAmount());
        assertEquals(OrderStatus.PARTIALLY_FILLED, partiallyFilledMaker.getStatus());
        assertTrue(engine.hasActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(2L));
        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));

        engine.processOrder(new Order(3L, 102L, OrderSide.BUY, 100L, 5L));

        assertNull(engine.findActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(1L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertEquals(0, countOrdersInBook(new BookLevel(OrderSide.SELL, 100L)));
    }

    @Test
    void shouldRemoveEmptyPriceLevelsImmediately() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 100L, 5L));

        engine.cancelOrder(1L);
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 101L));
        assertEquals(100L, engine.getBestBidPrice());

        engine.processOrder(new Order(3L, 201L, OrderSide.SELL, 100L, 5L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertNull(engine.getBestBidPrice());
    }

    @Test
    void shouldRemoveGhostOrdersAfterCancel() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 100L, 5L));

        engine.cancelOrder(1L);

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.findActiveOrder(1L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 101L));
        assertEquals(List.of(2L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(1, countOrdersInMap());
        assertEquals(1, countOrdersInBook(
                new BookLevel(OrderSide.BUY, 101L),
                new BookLevel(OrderSide.BUY, 100L)
        ));
    }

    @Test
    void shouldMaintainFifoForSamePriceLevel() {
        engine.processOrder(new Order(1L, 201L, OrderSide.SELL, 100L, 5L));
        engine.processOrder(new Order(2L, 202L, OrderSide.SELL, 100L, 5L));
        engine.processOrder(new Order(3L, 101L, OrderSide.BUY, 100L, 5L));

        TradeEvent firstTrade = tradeEventPublisher.tradeEvents.get(0);
        assertEquals(1L, firstTrade.getMakerOrderId());
        assertFalse(engine.hasActiveOrder(1L));
        assertTrue(engine.hasActiveOrder(2L));
        assertEquals(List.of(2L), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
    }

    private int countOrdersInBook(BookLevel... levels) {
        int total = 0;
        for (BookLevel level : levels) {
            total += engine.getOrderIdsAtPrice(level.side(), level.price()).size();
        }
        return total;
    }

    private int countOrdersInMap() {
        return engine.getActiveOrderCount();
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
