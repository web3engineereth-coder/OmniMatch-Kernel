package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryAccountServiceTest {

    @Test
    void shouldReserveBuyOrderFromAvailableCash() {
        InMemoryAccountService service = new InMemoryAccountService();
        service.createAccount(1001L, 5_000L, 0L);
        Order buyOrder = new Order(1L, 1001L, OrderSide.BUY, 100L, 10L);

        boolean reserved = service.reserveForOrder(buyOrder);
        Account account = service.getAccount(1001L);

        assertTrue(reserved);
        assertEquals(4_000L, account.getAvailableCash());
        assertEquals(1_000L, account.getFrozenCash());
        assertEquals(0L, account.getAvailableAsset());
        assertEquals(0L, account.getFrozenAsset());
    }

    @Test
    void shouldReserveSellOrderFromAvailableAsset() {
        InMemoryAccountService service = new InMemoryAccountService();
        service.createAccount(1002L, 0L, 50L);
        Order sellOrder = new Order(2L, 1002L, OrderSide.SELL, 100L, 10L);

        boolean reserved = service.reserveForOrder(sellOrder);
        Account account = service.getAccount(1002L);

        assertTrue(reserved);
        assertEquals(0L, account.getAvailableCash());
        assertEquals(0L, account.getFrozenCash());
        assertEquals(40L, account.getAvailableAsset());
        assertEquals(10L, account.getFrozenAsset());
    }

    @Test
    void shouldReturnFalseWhenBalanceIsInsufficient() {
        InMemoryAccountService service = new InMemoryAccountService();
        service.createAccount(1003L, 500L, 5L);

        assertFalse(service.reserveForOrder(new Order(3L, 1003L, OrderSide.BUY, 100L, 10L)));
        assertFalse(service.reserveForOrder(new Order(4L, 1003L, OrderSide.SELL, 100L, 10L)));
    }

    @Test
    void shouldReleaseReservedAmountOnCancel() {
        InMemoryAccountService service = new InMemoryAccountService();
        service.createAccount(1004L, 2_000L, 20L);

        Order buyOrder = new Order(5L, 1004L, OrderSide.BUY, 100L, 5L);
        service.reserveForOrder(buyOrder);
        service.releaseOnCancel(buyOrder);

        Account account = service.getAccount(1004L);
        assertEquals(2_000L, account.getAvailableCash());
        assertEquals(0L, account.getFrozenCash());

        Order sellOrder = new Order(6L, 1004L, OrderSide.SELL, 100L, 4L);
        service.reserveForOrder(sellOrder);
        service.releaseOnCancel(sellOrder);

        assertEquals(20L, account.getAvailableAsset());
        assertEquals(0L, account.getFrozenAsset());
    }

    @Test
    void shouldSettleTradeBetweenBuyerAndSeller() {
        InMemoryAccountService service = new InMemoryAccountService();
        service.createAccount(2001L, 10_000L, 0L);
        service.createAccount(2002L, 0L, 20L);

        Order buyOrder = new Order(7L, 2001L, OrderSide.BUY, 100L, 5L);
        Order sellOrder = new Order(8L, 2002L, OrderSide.SELL, 100L, 5L);

        assertTrue(service.reserveForOrder(buyOrder));
        assertTrue(service.reserveForOrder(sellOrder));

        buyOrder.fill(5L);
        sellOrder.fill(5L);

        TradeEvent tradeEvent = new TradeEvent(
                sellOrder.getOrderId(),
                buyOrder.getOrderId(),
                buyOrder.getUserId(),
                sellOrder.getUserId(),
                100L,
                5L,
                0L,
                0L,
                OrderStatus.FILLED,
                OrderStatus.FILLED
        );

        service.settleTrade(tradeEvent);

        Account buyer = service.getAccount(2001L);
        Account seller = service.getAccount(2002L);

        assertEquals(9_500L, buyer.getAvailableCash());
        assertEquals(0L, buyer.getFrozenCash());
        assertEquals(5L, buyer.getAvailableAsset());
        assertEquals(0L, buyer.getFrozenAsset());

        assertEquals(500L, seller.getAvailableCash());
        assertEquals(0L, seller.getFrozenCash());
        assertEquals(15L, seller.getAvailableAsset());
        assertEquals(0L, seller.getFrozenAsset());
    }
}
