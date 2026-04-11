package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.OrderCanceledEventPublisher;
import cn.inlook.cex.domain.service.OrderRejectedEventPublisher;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineCommandEventTest {

    @Test
    void shouldHandlePlaceOrderCommandAndEnterBook() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 1_000L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);

        PlaceOrderCommand command = new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 11L);

        engine.handle(command);

        Order activeOrder = engine.findActiveOrder(1L);
        assertTrue(engine.hasActiveOrder(1L));
        assertEquals(1, engine.getActiveOrderCount());
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals("BTC-USDT", activeOrder.getSymbol());
        assertEquals(11L, activeOrder.getTimestamp());
        assertEquals(500L, accountService.getAccount(1001L).getFrozenCash());
        assertEquals(500L, accountService.getAccount(1001L).getAvailableCash());
        assertTrue(tradePublisher.events.isEmpty());
        assertTrue(canceledPublisher.events.isEmpty());
        assertTrue(rejectedPublisher.events.isEmpty());
    }

    @Test
    void shouldHandleCancelOrderCommandAndReleaseFrozenBalance() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 1_000L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);

        engine.handle(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 11L));
        engine.handle(new CancelOrderCommand("BTC-USDT", 1L, 1001L, 12L));

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.getBestBidPrice());
        assertEquals(0L, accountService.getAccount(1001L).getFrozenCash());
        assertEquals(1_000L, accountService.getAccount(1001L).getAvailableCash());
        assertEquals(1, canceledPublisher.events.size());
        OrderCanceledEvent event = canceledPublisher.events.get(0);
        assertEquals("BTC-USDT", event.getSymbol());
        assertEquals(1L, event.getOrderId());
        assertEquals(1001L, event.getUserId());
        assertEquals(5L, event.getRemainingQuantity());
        assertEquals(12L, event.getTimestamp());
        assertTrue(rejectedPublisher.events.isEmpty());
    }

    @Test
    void shouldPublishOrderRejectedEventWhenReserveFails() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 100L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);

        engine.handle(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 100L, 2L, 21L));

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.getBestBidPrice());
        assertEquals(0L, accountService.getAccount(1001L).getFrozenCash());
        assertEquals(100L, accountService.getAccount(1001L).getAvailableCash());
        assertEquals(1, rejectedPublisher.events.size());
        OrderRejectedEvent event = rejectedPublisher.events.get(0);
        assertEquals("BTC-USDT", event.getSymbol());
        assertEquals(1L, event.getOrderId());
        assertEquals(1001L, event.getUserId());
        assertEquals(OrderRejectReason.INSUFFICIENT_BALANCE, event.getReason());
        assertEquals(21L, event.getTimestamp());
        assertTrue(canceledPublisher.events.isEmpty());
        assertTrue(tradePublisher.events.isEmpty());
    }

    @Test
    void shouldKeepExistingMatchingSemanticsWhenCommandsCross() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);

        engine.handle(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.SELL, 100L, 10L, 31L));
        engine.handle(new PlaceOrderCommand(2L, 1002L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 32L));

        assertEquals(1, tradePublisher.events.size());
        assertTrue(engine.hasActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(2L));
        assertEquals(100L, engine.getBestAskPrice());
        assertEquals(5L, engine.findActiveOrder(1L).getRemainingAmount());
        assertTrue(canceledPublisher.events.isEmpty());
        assertTrue(rejectedPublisher.events.isEmpty());
        assertEquals(500L, accountService.getAccount(1001L).getAvailableCash());
        assertEquals(5L, accountService.getAccount(1001L).getFrozenAsset());
        assertEquals(5L, accountService.getAccount(1002L).getAvailableAsset());
        assertEquals(500L, accountService.getAccount(1002L).getAvailableCash());
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> events = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            events.add(tradeEvent);
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
