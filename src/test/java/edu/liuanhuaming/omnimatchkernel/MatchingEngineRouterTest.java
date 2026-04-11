package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.AccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.MatchingEngineRouter;
import cn.inlook.cex.domain.service.OrderCanceledEventPublisher;
import cn.inlook.cex.domain.service.OrderRejectedEventPublisher;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineRouterTest {

    @Test
    void shouldRouteDifferentSymbolsToDifferentMatchingEngines() {
        MatchingEngineRouter router = new MatchingEngineRouter(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher()
        );

        router.processOrder(new Order(1L, 101L, "BTC-USDT", OrderSide.BUY, 100L, 5L));
        router.processOrder(new Order(2L, 102L, "ETH-USDT", OrderSide.BUY, 200L, 5L));

        MatchingEngine btcEngine = router.getEngine("BTC-USDT");
        MatchingEngine ethEngine = router.getEngine("ETH-USDT");

        assertNotNull(btcEngine);
        assertNotNull(ethEngine);
        assertNotSame(btcEngine, ethEngine);
        assertEquals(List.of(1L), router.getOrderIdsAtPrice("BTC-USDT", OrderSide.BUY, 100L));
        assertEquals(List.of(2L), router.getOrderIdsAtPrice("ETH-USDT", OrderSide.BUY, 200L));
        assertEquals(List.of(), router.getOrderIdsAtPrice("BTC-USDT", OrderSide.BUY, 200L));
        assertEquals(List.of(), router.getOrderIdsAtPrice("ETH-USDT", OrderSide.BUY, 100L));
    }

    @Test
    void shouldMaintainBestPricesIndependentlyPerSymbol() {
        MatchingEngineRouter router = new MatchingEngineRouter(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher()
        );

        router.processOrder(new Order(1L, 101L, "BTC-USDT", OrderSide.BUY, 101L, 5L));
        router.processOrder(new Order(2L, 102L, "BTC-USDT", OrderSide.SELL, 105L, 5L));
        router.processOrder(new Order(3L, 103L, "ETH-USDT", OrderSide.BUY, 201L, 5L));
        router.processOrder(new Order(4L, 104L, "ETH-USDT", OrderSide.SELL, 205L, 5L));

        assertEquals(101L, router.getBestBidPrice("BTC-USDT"));
        assertEquals(105L, router.getBestAskPrice("BTC-USDT"));
        assertEquals(201L, router.getBestBidPrice("ETH-USDT"));
        assertEquals(205L, router.getBestAskPrice("ETH-USDT"));
    }

    @Test
    void shouldCancelOnlyWithinTargetSymbol() {
        MatchingEngineRouter router = new MatchingEngineRouter(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher()
        );

        router.processOrder(new Order(1L, 101L, "BTC-USDT", OrderSide.BUY, 101L, 5L));
        router.processOrder(new Order(2L, 102L, "BTC-USDT", OrderSide.BUY, 100L, 5L));
        router.processOrder(new Order(3L, 103L, "ETH-USDT", OrderSide.BUY, 201L, 5L));

        router.cancelOrder("BTC-USDT", 1L);

        assertFalse(router.hasActiveOrder("BTC-USDT", 1L));
        assertTrue(router.hasActiveOrder("BTC-USDT", 2L));
        assertTrue(router.hasActiveOrder("ETH-USDT", 3L));
        assertEquals(List.of(), router.getOrderIdsAtPrice("BTC-USDT", OrderSide.BUY, 101L));
        assertEquals(List.of(2L), router.getOrderIdsAtPrice("BTC-USDT", OrderSide.BUY, 100L));
        assertEquals(List.of(3L), router.getOrderIdsAtPrice("ETH-USDT", OrderSide.BUY, 201L));
        assertEquals(100L, router.getBestBidPrice("BTC-USDT"));
        assertEquals(201L, router.getBestBidPrice("ETH-USDT"));
    }

    @Test
    void shouldRoutePlaceOrderCommandBySymbol() {
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngineRouter router = new MatchingEngineRouter(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher(),
                canceledPublisher,
                rejectedPublisher
        );

        router.handle(new PlaceOrderCommand(1L, 101L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 11L));
        router.handle(new PlaceOrderCommand(2L, 102L, "ETH-USDT", OrderSide.SELL, 200L, 5L, 12L));

        assertEquals(List.of(1L), router.getOrderIdsAtPrice("BTC-USDT", OrderSide.BUY, 100L));
        assertEquals(List.of(2L), router.getOrderIdsAtPrice("ETH-USDT", OrderSide.SELL, 200L));
        assertEquals(100L, router.getBestBidPrice("BTC-USDT"));
        assertEquals(200L, router.getBestAskPrice("ETH-USDT"));
        assertTrue(canceledPublisher.events.isEmpty());
        assertTrue(rejectedPublisher.events.isEmpty());
    }

    @Test
    void shouldCancelOnlyWithinTargetSymbolViaCommand() {
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngineRouter router = new MatchingEngineRouter(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher(),
                canceledPublisher,
                rejectedPublisher
        );

        router.handle(new PlaceOrderCommand(1L, 101L, "BTC-USDT", OrderSide.BUY, 101L, 5L, 11L));
        router.handle(new PlaceOrderCommand(2L, 102L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 12L));
        router.handle(new PlaceOrderCommand(3L, 103L, "ETH-USDT", OrderSide.BUY, 201L, 5L, 13L));

        router.handle(new CancelOrderCommand("BTC-USDT", 1L, 101L, 14L));

        assertFalse(router.hasActiveOrder("BTC-USDT", 1L));
        assertTrue(router.hasActiveOrder("BTC-USDT", 2L));
        assertTrue(router.hasActiveOrder("ETH-USDT", 3L));
        assertEquals(1, canceledPublisher.events.size());
        OrderCanceledEvent event = canceledPublisher.events.get(0);
        assertEquals("BTC-USDT", event.getSymbol());
        assertEquals(1L, event.getOrderId());
        assertEquals(5L, event.getRemainingQuantity());
        assertEquals(14L, event.getTimestamp());
        assertTrue(rejectedPublisher.events.isEmpty());
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

    private static class RecordingOrderCanceledEventPublisher implements OrderCanceledEventPublisher {
        private final List<OrderCanceledEvent> events = new ArrayList<>();

        @Override
        public void publish(OrderCanceledEvent event) {
            events.add(event);
        }
    }

    private static class RecordingOrderRejectedEventPublisher implements OrderRejectedEventPublisher {
        private final List<OrderRejectedEvent> events = new ArrayList<>();

        @Override
        public void publish(OrderRejectedEvent event) {
            events.add(event);
        }
    }
}
