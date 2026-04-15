package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.AccountView;
import cn.inlook.cex.domain.model.BookSnapshotView;
import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.EngineEvent;
import cn.inlook.cex.domain.model.EngineEventType;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.OrderView;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.service.MatchingGateway;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MatchingGatewayTest {

    @Test
    void shouldExposeUnifiedGatewayForCommandQueryAndEventReads() {
        MatchingGateway gateway = new MatchingGateway();
        gateway.createAccount(1001L, 0L, 10L);
        gateway.createAccount(1002L, 1_000L, 0L);
        gateway.createAccount(1003L, 100L, 0L);

        gateway.handle(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", cn.inlook.cex.domain.model.OrderSide.SELL, 100L, 10L, 11L));
        gateway.handle(new PlaceOrderCommand(2L, 1002L, "BTC-USDT", cn.inlook.cex.domain.model.OrderSide.BUY, 100L, 5L, 12L));
        gateway.handle(new CancelOrderCommand("BTC-USDT", 1L, 1001L, 13L));
        gateway.handle(new PlaceOrderCommand(3L, 1003L, "BTC-USDT", cn.inlook.cex.domain.model.OrderSide.BUY, 100L, 2L, 14L));

        BookSnapshotView snapshot = gateway.getBookSnapshot("BTC-USDT");
        assertNull(snapshot.getBestBid());
        assertNull(snapshot.getBestAsk());

        OrderView canceledOrder = gateway.getOrder("BTC-USDT", 1L);
        assertEquals(OrderStatus.CANCELED, canceledOrder.getStatus());

        OrderView rejectedOrder = gateway.getOrder("BTC-USDT", 3L);
        assertEquals(OrderStatus.REJECTED, rejectedOrder.getStatus());
        assertEquals(OrderRejectReason.INSUFFICIENT_BALANCE, rejectedOrder.getRejectReason());

        AccountView seller = gateway.getAccount(1001L);
        AccountView buyer = gateway.getAccount(1002L);
        assertEquals(500L, seller.getAvailableCash());
        assertEquals(5L, seller.getAvailableAsset());
        assertEquals(500L, buyer.getAvailableCash());
        assertEquals(5L, buyer.getAvailableAsset());

        List<EngineEvent> events = gateway.getEvents();
        assertEquals(3, events.size());
        assertEquals(EngineEventType.TRADE, events.get(0).getEventType());
        assertEquals(EngineEventType.ORDER_CANCELED, events.get(1).getEventType());
        assertEquals(EngineEventType.ORDER_REJECTED, events.get(2).getEventType());
    }
}
