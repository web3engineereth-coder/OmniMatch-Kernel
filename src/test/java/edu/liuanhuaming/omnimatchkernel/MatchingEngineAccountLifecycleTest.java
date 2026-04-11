package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
        assertEquals(1, engine.getActiveOrderCount());
        assertEquals(List.of(1L), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertEquals(100L, engine.getBestAskPrice());
        assertNull(engine.getBestBidPrice());
        assertEquals(1, publisher.events.size());

        engine.cancelOrder(1L);

        Account sellerFinal = accountService.getAccount(1001L);
        Account buyerFinal = accountService.getAccount(1002L);

        assertEquals(500L, sellerFinal.getAvailableCash());
        assertEquals(5L, sellerFinal.getAvailableAsset());
        assertNotEquals(10L, sellerFinal.getAvailableAsset(),
                "Cancel must release only the remaining 5 asset units, not the full original order.");
        assertEquals(0L, sellerFinal.getFrozenAsset());

        assertEquals(500L, buyerFinal.getAvailableCash());
        assertEquals(5L, buyerFinal.getAvailableAsset());
        assertEquals(0L, buyerFinal.getFrozenCash());

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.findActiveOrder(1L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertNull(engine.getBestAskPrice());
        assertEquals(0, engine.getActiveOrderCount());
        engine.assertInvariant();

        TradeEvent tradeEvent = publisher.events.get(0);
        assertEquals(1L, tradeEvent.getMakerOrderId());
        assertEquals(2L, tradeEvent.getTakerOrderId());
        assertEquals(5L, tradeEvent.getQuantity());
        assertEquals(5L, tradeEvent.getMakerRemainingQty());
        assertEquals(0L, tradeEvent.getTakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, tradeEvent.getMakerStatus());
        assertEquals(OrderStatus.FILLED, tradeEvent.getTakerStatus());
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
        assertEquals(1, engine.getActiveOrderCount());
        assertEquals(List.of(3L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(100L, engine.getBestBidPrice());
        assertNull(engine.getBestAskPrice());
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
        assertEquals(0L, tradeEvent.getTakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, tradeEvent.getMakerStatus());
        assertEquals(OrderStatus.FILLED, tradeEvent.getTakerStatus());
    }

    @Test
    void shouldReleaseOnlyFinalRemainingAmountAfterMultiplePartialFillsAndCancel() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);
        accountService.createAccount(1003L, 1_000L, 0L);

        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        Order sellOrder = new Order(5L, 1001L, OrderSide.SELL, 100L, 10L);
        Order firstBuy = new Order(6L, 1002L, OrderSide.BUY, 100L, 3L);
        Order secondBuy = new Order(7L, 1003L, OrderSide.BUY, 100L, 2L);

        engine.processOrder(sellOrder);
        engine.processOrder(firstBuy);
        engine.processOrder(secondBuy);

        Account sellerAfterTrades = accountService.getAccount(1001L);
        Account firstBuyer = accountService.getAccount(1002L);
        Account secondBuyer = accountService.getAccount(1003L);

        assertEquals(500L, sellerAfterTrades.getAvailableCash());
        assertEquals(0L, sellerAfterTrades.getAvailableAsset());
        assertEquals(5L, sellerAfterTrades.getFrozenAsset());

        assertEquals(700L, firstBuyer.getAvailableCash());
        assertEquals(0L, firstBuyer.getFrozenCash());
        assertEquals(3L, firstBuyer.getAvailableAsset());

        assertEquals(800L, secondBuyer.getAvailableCash());
        assertEquals(0L, secondBuyer.getFrozenCash());
        assertEquals(2L, secondBuyer.getAvailableAsset());

        assertTrue(engine.hasActiveOrder(5L));
        assertEquals(List.of(5L), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertEquals(100L, engine.getBestAskPrice());
        assertEquals(2, publisher.events.size());

        engine.cancelOrder(5L);

        Account sellerFinal = accountService.getAccount(1001L);

        assertEquals(500L, sellerFinal.getAvailableCash());
        assertEquals(5L, sellerFinal.getAvailableAsset());
        assertEquals(0L, sellerFinal.getFrozenAsset());

        assertFalse(engine.hasActiveOrder(5L));
        assertNull(engine.findActiveOrder(5L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
        assertNull(engine.getBestAskPrice());
        assertEquals(0, engine.getActiveOrderCount());

        TradeEvent firstTrade = publisher.events.get(0);
        TradeEvent secondTrade = publisher.events.get(1);

        assertEquals(5L, firstTrade.getMakerOrderId());
        assertEquals(6L, firstTrade.getTakerOrderId());
        assertEquals(3L, firstTrade.getQuantity());
        assertEquals(7L, firstTrade.getMakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, firstTrade.getMakerStatus());

        assertEquals(5L, secondTrade.getMakerOrderId());
        assertEquals(7L, secondTrade.getTakerOrderId());
        assertEquals(2L, secondTrade.getQuantity());
        assertEquals(5L, secondTrade.getMakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, secondTrade.getMakerStatus());
    }

    @Test
    void shouldReleaseOnlyFinalRemainingFrozenCashAfterMultiplePartialFillsAndCancel() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 0L, 10L);
        accountService.createAccount(1002L, 1_000L, 0L);
        accountService.createAccount(1003L, 0L, 10L);

        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);

        Order buyOrder = new Order(8L, 1002L, OrderSide.BUY, 100L, 10L);
        Order firstSell = new Order(9L, 1001L, OrderSide.SELL, 100L, 3L);
        Order secondSell = new Order(10L, 1003L, OrderSide.SELL, 100L, 2L);

        engine.processOrder(buyOrder);
        engine.processOrder(firstSell);
        engine.processOrder(secondSell);

        Account buyerAfterTrades = accountService.getAccount(1002L);
        Account firstSeller = accountService.getAccount(1001L);
        Account secondSeller = accountService.getAccount(1003L);

        assertEquals(0L, buyerAfterTrades.getAvailableCash());
        assertEquals(500L, buyerAfterTrades.getFrozenCash());
        assertEquals(5L, buyerAfterTrades.getAvailableAsset());

        assertEquals(300L, firstSeller.getAvailableCash());
        assertEquals(7L, firstSeller.getAvailableAsset());
        assertEquals(0L, firstSeller.getFrozenAsset());

        assertEquals(200L, secondSeller.getAvailableCash());
        assertEquals(8L, secondSeller.getAvailableAsset());
        assertEquals(0L, secondSeller.getFrozenAsset());

        assertTrue(engine.hasActiveOrder(8L));
        assertEquals(List.of(8L), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(2, publisher.events.size());

        engine.cancelOrder(8L);

        Account buyerFinal = accountService.getAccount(1002L);

        assertEquals(500L, buyerFinal.getAvailableCash());
        assertEquals(0L, buyerFinal.getFrozenCash());
        assertEquals(5L, buyerFinal.getAvailableAsset());

        assertFalse(engine.hasActiveOrder(8L));
        assertNull(engine.findActiveOrder(8L));
        assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.BUY, 100L));
        assertNull(engine.getBestBidPrice());
        assertEquals(0, engine.getActiveOrderCount());

        TradeEvent firstTrade = publisher.events.get(0);
        TradeEvent secondTrade = publisher.events.get(1);

        assertEquals(8L, firstTrade.getMakerOrderId());
        assertEquals(9L, firstTrade.getTakerOrderId());
        assertEquals(3L, firstTrade.getQuantity());
        assertEquals(7L, firstTrade.getMakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, firstTrade.getMakerStatus());

        assertEquals(8L, secondTrade.getMakerOrderId());
        assertEquals(10L, secondTrade.getTakerOrderId());
        assertEquals(2L, secondTrade.getQuantity());
        assertEquals(5L, secondTrade.getMakerRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, secondTrade.getMakerStatus());
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> events = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            events.add(tradeEvent);
        }
    }
}
