package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.AccountView;
import cn.inlook.cex.domain.model.BookLevelView;
import cn.inlook.cex.domain.model.BookSnapshotView;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.OrderView;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngineRouter;
import cn.inlook.cex.domain.service.MatchingQueryService;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingQueryServiceTest {

    @Test
    void shouldBuildIndependentBookSnapshotsPerSymbol() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 5_000L, 10L);
        accountService.createAccount(1002L, 5_000L, 10L);
        accountService.createAccount(1003L, 5_000L, 10L);
        accountService.createAccount(1004L, 5_000L, 10L);

        MatchingEngineRouter router = new MatchingEngineRouter(accountService, new RecordingTradeEventPublisher());
        MatchingQueryService queryService = new MatchingQueryService(router, accountService);

        router.handle(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 101L, 5L, 11L));
        router.handle(new PlaceOrderCommand(2L, 1002L, "BTC-USDT", OrderSide.BUY, 100L, 3L, 12L));
        router.handle(new PlaceOrderCommand(3L, 1003L, "BTC-USDT", OrderSide.SELL, 105L, 4L, 13L));
        router.handle(new PlaceOrderCommand(4L, 1004L, "ETH-USDT", OrderSide.SELL, 205L, 6L, 14L));

        BookSnapshotView btcSnapshot = queryService.getBookSnapshot("BTC-USDT");
        BookSnapshotView ethSnapshot = queryService.getBookSnapshot("ETH-USDT");

        assertEquals("BTC-USDT", btcSnapshot.getSymbol());
        assertEquals(101L, btcSnapshot.getBestBid());
        assertEquals(105L, btcSnapshot.getBestAsk());
        assertEquals(2, btcSnapshot.getBidLevels().size());
        assertEquals(1, btcSnapshot.getAskLevels().size());

        BookLevelView topBtcBid = btcSnapshot.getBidLevels().get(0);
        assertEquals(101L, topBtcBid.getPrice());
        assertEquals(1, topBtcBid.getOrderCount());
        assertEquals(5L, topBtcBid.getTotalRemainingQty());

        assertEquals("ETH-USDT", ethSnapshot.getSymbol());
        assertNull(ethSnapshot.getBestBid());
        assertEquals(205L, ethSnapshot.getBestAsk());
        assertTrue(ethSnapshot.getBidLevels().isEmpty());
        assertEquals(1, ethSnapshot.getAskLevels().size());
        assertEquals(205L, ethSnapshot.getAskLevels().get(0).getPrice());

        router.assertInvariantAll();
    }

    @Test
    void shouldExposeActiveOrderAndAccountViewsAfterPartialFillAndCancel() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(2001L, 0L, 10L);
        accountService.createAccount(2002L, 1_000L, 0L);

        MatchingEngineRouter router = new MatchingEngineRouter(accountService, new RecordingTradeEventPublisher());
        MatchingQueryService queryService = new MatchingQueryService(router, accountService);

        router.handle(new PlaceOrderCommand(10L, 2001L, "BTC-USDT", OrderSide.SELL, 100L, 10L, 21L));
        router.handle(new PlaceOrderCommand(11L, 2002L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 22L));

        OrderView activeSellOrder = queryService.getActiveOrder("BTC-USDT", 10L);
        assertNotNull(activeSellOrder);
        assertEquals(10L, activeSellOrder.getOrderId());
        assertEquals(5L, activeSellOrder.getRemainingQty());
        assertEquals(OrderStatus.PARTIALLY_FILLED, activeSellOrder.getStatus());

        AccountView sellerAfterTrade = queryService.getAccount(2001L);
        AccountView buyerAfterTrade = queryService.getAccount(2002L);
        assertEquals(500L, sellerAfterTrade.getAvailableCash());
        assertEquals(5L, sellerAfterTrade.getFrozenAsset());
        assertEquals(500L, buyerAfterTrade.getAvailableCash());
        assertEquals(5L, buyerAfterTrade.getAvailableAsset());

        router.handle(new cn.inlook.cex.domain.model.CancelOrderCommand("BTC-USDT", 10L, 2001L, 23L));

        assertNull(queryService.getActiveOrder("BTC-USDT", 10L));
        OrderView canceledOrder = queryService.getOrder("BTC-USDT", 10L);
        assertNotNull(canceledOrder);
        assertEquals(OrderStatus.CANCELED, canceledOrder.getStatus());
        assertEquals(0L, canceledOrder.getRemainingQty());
        BookSnapshotView snapshotAfterCancel = queryService.getBookSnapshot("BTC-USDT");
        assertNull(snapshotAfterCancel.getBestBid());
        assertNull(snapshotAfterCancel.getBestAsk());
        assertTrue(snapshotAfterCancel.getBidLevels().isEmpty());
        assertTrue(snapshotAfterCancel.getAskLevels().isEmpty());

        AccountView sellerFinal = queryService.getAccount(2001L);
        assertEquals(500L, sellerFinal.getAvailableCash());
        assertEquals(5L, sellerFinal.getAvailableAsset());
        assertEquals(0L, sellerFinal.getFrozenAsset());
        assertFalse(router.hasActiveOrder("BTC-USDT", 10L));
        router.assertInvariantAll();
    }

    @Test
    void shouldExposeFilledOrderLifecycleViewAfterExactMatch() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(3001L, 0L, 5L);
        accountService.createAccount(3002L, 1_000L, 0L);

        MatchingEngineRouter router = new MatchingEngineRouter(accountService, new RecordingTradeEventPublisher());
        MatchingQueryService queryService = new MatchingQueryService(router, accountService);

        router.handle(new PlaceOrderCommand(20L, 3001L, "BTC-USDT", OrderSide.SELL, 100L, 5L, 31L));
        router.handle(new PlaceOrderCommand(21L, 3002L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 32L));

        assertNull(queryService.getActiveOrder("BTC-USDT", 20L));
        assertNull(queryService.getActiveOrder("BTC-USDT", 21L));

        OrderView makerOrder = queryService.getOrder("BTC-USDT", 20L);
        OrderView takerOrder = queryService.getOrder("BTC-USDT", 21L);

        assertNotNull(makerOrder);
        assertNotNull(takerOrder);
        assertEquals(OrderStatus.FILLED, makerOrder.getStatus());
        assertEquals(0L, makerOrder.getRemainingQty());
        assertEquals(OrderStatus.FILLED, takerOrder.getStatus());
        assertEquals(0L, takerOrder.getRemainingQty());

        BookSnapshotView snapshot = queryService.getBookSnapshot("BTC-USDT");
        assertTrue(snapshot.getBidLevels().isEmpty());
        assertTrue(snapshot.getAskLevels().isEmpty());
        assertNull(snapshot.getBestBid());
        assertNull(snapshot.getBestAsk());
        router.assertInvariantAll();
    }

    @Test
    void shouldExposeRejectedOrderLifecycleViewWhenBalanceIsInsufficient() {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(4001L, 100L, 0L);

        MatchingEngineRouter router = new MatchingEngineRouter(accountService, new RecordingTradeEventPublisher());
        MatchingQueryService queryService = new MatchingQueryService(router, accountService);

        router.handle(new PlaceOrderCommand(30L, 4001L, "BTC-USDT", OrderSide.BUY, 100L, 2L, 41L));

        assertNull(queryService.getActiveOrder("BTC-USDT", 30L));

        OrderView rejectedOrder = queryService.getOrder("BTC-USDT", 30L);
        assertNotNull(rejectedOrder);
        assertEquals(OrderStatus.REJECTED, rejectedOrder.getStatus());
        assertEquals(2L, rejectedOrder.getRemainingQty());
        assertEquals(OrderRejectReason.INSUFFICIENT_BALANCE, rejectedOrder.getRejectReason());

        BookSnapshotView snapshot = queryService.getBookSnapshot("BTC-USDT");
        assertTrue(snapshot.getBidLevels().isEmpty());
        assertTrue(snapshot.getAskLevels().isEmpty());
        assertNull(snapshot.getBestBid());
        assertNull(snapshot.getBestAsk());
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> tradeEvents = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            tradeEvents.add(tradeEvent);
        }
    }
}
