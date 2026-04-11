package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineAccountLifecycleTest {

    @Test
    void shouldSettlePartialFillAndReleaseOnlyRemainingAmountOnCancel() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);

        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        Order sellOrder = new Order(1L, 1001L, OrderSide.SELL, 100L, 10L);
        Order buyOrder = new Order(2L, 1002L, OrderSide.BUY, 100L, 5L);

        engine.processOrder(sellOrder);
        engine.processOrder(buyOrder);

        Account sellerAfterTrade = accountService.getAccount(1001L);
        Account buyerAfterTrade = accountService.getAccount(1002L);

        assertEquals(500L, sellerAfterTrade.getAvailableCash());
        assertEquals(0L, sellerAfterTrade.getAvailableAsset());
        assertEquals(5L, sellerAfterTrade.getFrozenAsset());

        assertEquals(500L, buyerAfterTrade.getAvailableCash());
        assertEquals(0L, buyerAfterTrade.getFrozenCash());
        assertEquals(5L, buyerAfterTrade.getAvailableAsset());

        assertTrue(engine.hasActiveOrder(1L));
        assertFalse(engine.hasActiveOrder(2L));
        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertEquals(1, publisher.events.size());

        engine.cancelOrder(1L);

        Account sellerFinal = accountService.getAccount(1001L);
        Account buyerFinal = accountService.getAccount(1002L);

        assertEquals(500L, sellerFinal.getAvailableCash());
        assertEquals(5L, sellerFinal.getAvailableAsset());
        assertEquals(0L, sellerFinal.getFrozenAsset());

        assertEquals(500L, buyerFinal.getAvailableCash());
        assertEquals(5L, buyerFinal.getAvailableAsset());
        assertEquals(0L, buyerFinal.getFrozenCash());

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.findActiveOrder(1L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertNull(engine.getBestAskPrice());
        assertEquals(0, engine.getActiveOrderCount());

        TradeEvent tradeEvent = publisher.events.get(0);
        assertEquals(1L, tradeEvent.getMakerOrderId());
        assertEquals(2L, tradeEvent.getTakerOrderId());
        assertEquals(5L, tradeEvent.getQuantity());
        assertEquals(5L, tradeEvent.getMakerRemainingQty());
    }

    @Test
    void shouldSettlePartialFillAndReleaseOnlyRemainingFrozenCashOnBuyCancel() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);

        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        Order buyOrder = new Order(3L, 1002L, OrderSide.BUY, 100L, 10L);
        Order sellOrder = new Order(4L, 1001L, OrderSide.SELL, 100L, 5L);

        engine.processOrder(buyOrder);
        engine.processOrder(sellOrder);

        Account sellerAfterTrade = accountService.getAccount(1001L);
        Account buyerAfterTrade = accountService.getAccount(1002L);

        assertEquals(500L, sellerAfterTrade.getAvailableCash());
        assertEquals(5L, sellerAfterTrade.getAvailableAsset());
        assertEquals(0L, sellerAfterTrade.getFrozenAsset());

        assertEquals(0L, buyerAfterTrade.getAvailableCash());
        assertEquals(500L, buyerAfterTrade.getFrozenCash());
        assertEquals(5L, buyerAfterTrade.getAvailableAsset());

        assertTrue(engine.hasActiveOrder(3L));
        assertFalse(engine.hasActiveOrder(4L));
        assertEquals(List.of(3L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(1, publisher.events.size());

        engine.cancelOrder(3L);

        Account sellerFinal = accountService.getAccount(1001L);
        Account buyerFinal = accountService.getAccount(1002L);

        assertEquals(500L, sellerFinal.getAvailableCash());
        assertEquals(5L, sellerFinal.getAvailableAsset());
        assertEquals(0L, sellerFinal.getFrozenAsset());

        assertEquals(500L, buyerFinal.getAvailableCash());
        assertEquals(0L, buyerFinal.getFrozenCash());
        assertEquals(5L, buyerFinal.getAvailableAsset());

        assertFalse(engine.hasActiveOrder(3L));
        assertNull(engine.findActiveOrder(3L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertNull(engine.getBestBidPrice());
        assertEquals(0, engine.getActiveOrderCount());

        TradeEvent tradeEvent = publisher.events.get(0);
        assertEquals(3L, tradeEvent.getMakerOrderId());
        assertEquals(4L, tradeEvent.getTakerOrderId());
        assertEquals(5L, tradeEvent.getQuantity());
        assertEquals(5L, tradeEvent.getMakerRemainingQty());
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> events = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            events.add(tradeEvent);
        }
    }
}
