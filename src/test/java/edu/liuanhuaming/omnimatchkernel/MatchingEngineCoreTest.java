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

public class MatchingEngineCoreTest {

    private final RecordingAccountService accountService = new RecordingAccountService();
    private final RecordingTradeEventPublisher tradeEventPublisher = new RecordingTradeEventPublisher();
    private final MatchingEngine engine = new MatchingEngine(accountService, tradeEventPublisher);

    @Test
    void shouldEnterBookAndExposeBestBid() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 100L, 10L));

        assertEquals(1, engine.getActiveOrderCount());
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(OrderStatus.NEW, engine.findActiveOrder(1L).getStatus());
    }

    @Test
    void shouldCancelMiddleNodeThroughOrderMapPath() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 100L, 10L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 100L, 10L));
        engine.processOrder(new Order(3L, 103L, OrderSide.BUY, 100L, 10L));

        engine.cancelOrder(2L);

        assertFalse(engine.hasActiveOrder(2L));
        assertEquals(List.of(1L, 3L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(100L, engine.getBestBidPrice());
    }

    @Test
    void shouldKeepMakerPositionOnPartialFill() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 100L, 10L));
        engine.processOrder(new Order(2L, 201L, OrderSide.SELL, 100L, 4L));

        Order restingOrder = engine.findActiveOrder(1L);
        TradeEvent publishedEvent = tradeEventPublisher.tradeEvents.get(0);
        assertEquals(6L, restingOrder.getRemainingAmount());
        assertEquals(OrderStatus.PARTIALLY_FILLED, restingOrder.getStatus());
        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(1, accountService.tradeEvents.size());
        assertEquals(1, tradeEventPublisher.tradeEvents.size());
        assertEquals(1L, publishedEvent.getMakerOrderId());
        assertEquals(2L, publishedEvent.getTakerOrderId());
        assertEquals(4L, publishedEvent.getQuantity());
        assertEquals(100L, publishedEvent.getPrice());
        assertEquals(6L, publishedEvent.getMakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, publishedEvent.getMakerStatus());
        assertEquals(OrderStatus.FILLED, publishedEvent.getTakerStatus());
    }

    @Test
    void shouldRemoveFilledNodeAndClearEmptyPriceLevel() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 100L, 5L));

        engine.processOrder(new Order(3L, 201L, OrderSide.SELL, 101L, 5L));

        assertFalse(engine.hasActiveOrder(1L));
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 101L));
    }

    @Test
    void shouldUpdateBestPricesAcrossBothSides() {
        engine.processOrder(new Order(1L, 101L, OrderSide.SELL, 105L, 10L));
        engine.processOrder(new Order(2L, 102L, OrderSide.SELL, 103L, 10L));
        engine.processOrder(new Order(3L, 103L, OrderSide.BUY, 100L, 10L));

        assertEquals(103L, engine.getBestAskPrice());
        assertEquals(100L, engine.getBestBidPrice());

        engine.cancelOrder(2L);

        assertEquals(105L, engine.getBestAskPrice());
    }

    @Test
    void shouldFullyRemoveBothOrdersWhenTradeCompletesExactly() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 100L, 5L));
        engine.processOrder(new Order(2L, 201L, OrderSide.SELL, 100L, 5L));

        assertFalse(engine.hasActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(2L));
        assertNull(engine.getBestBidPrice());
        assertNull(engine.getBestAskPrice());
        assertTrue(accountService.tradeEvents.size() >= 1);
        assertTrue(tradeEventPublisher.tradeEvents.size() >= 1);
    }

    @Test
    void shouldNotPublishTradeEventWhenNoMatchOccurs() {
        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 100L, 5L));
        engine.processOrder(new Order(2L, 201L, OrderSide.SELL, 105L, 5L));

        assertEquals(0, tradeEventPublisher.tradeEvents.size());
        assertEquals(0, accountService.tradeEvents.size());
        assertEquals(2, engine.getActiveOrderCount());
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(105L, engine.getBestAskPrice());
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
