package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderBook;
import cn.inlook.cex.domain.model.OrderNode;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.PriceLevel;
import cn.inlook.cex.domain.model.TradeEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * [ZH] 核心撮合引擎 - 单线程事件驱动，内部使用 PriceLevel + OrderNode 结构
 * [EN] Core matching engine using a single-threaded event-driven flow with PriceLevel + OrderNode
 */
@Slf4j
public class MatchingEngine {

    private OrderBook bids;
    private OrderBook asks;
    private AccountService accountService;

    // [ZH] 全局订单节点索引，用于 O(1) 撤单
    // [EN] Global order-node index for O(1) cancellation
    private Map<Long, OrderNode> orderMap = new HashMap<>();

    private final int baseCurrency = 1;
    private final int quoteCurrency = 2;

    public MatchingEngine(BalanceManager balanceManager) {
        this(new BalanceManagerAccountService(balanceManager, 1, 2));
    }

    public MatchingEngine(AccountService accountService) {
        this.bids = new OrderBook(OrderSide.BUY);
        this.asks = new OrderBook(OrderSide.SELL);
        this.accountService = accountService;
    }

    public void processOrder(Order incomingOrder) {
        if (!accountService.reserveForOrder(incomingOrder)) {
            log.warn("Order {} rejected by account boundary.", incomingOrder.getOrderId());
            return;
        }

        if (incomingOrder.getSide() == OrderSide.BUY) {
            match(incomingOrder, asks);
        } else {
            match(incomingOrder, bids);
        }

        if (!incomingOrder.isFilled() && !incomingOrder.isCanceled()) {
            addLimitOrder(incomingOrder);
        }
    }

    public void cancelOrder(long orderId) {
        OrderNode node = orderMap.remove(orderId);
        if (node == null) {
            log.warn("Cancel failed: Order {} not found or already removed.", orderId);
            return;
        }

        node.cancel();
        getBook(node.getSide()).removeNode(node);
        accountService.releaseOnCancel(node.getOrder());
        log.info("Order {} canceled and detached from price level {}.", orderId, node.getPrice());
    }

    private void match(Order taker, OrderBook makerBook) {
        while (!makerBook.isEmpty() && !taker.isFilled()) {
            Long bestPrice = makerBook.getBestPrice();
            if (bestPrice == null || !isPriceMatch(taker, bestPrice)) {
                break;
            }

            PriceLevel bestLevel = makerBook.getBestLevel();
            while (bestLevel != null && bestLevel.getHead() != null && !taker.isFilled()) {
                OrderNode makerNode = bestLevel.getHead();
                long tradedQty = Math.min(taker.getRemainingAmount(), makerNode.getRemainingQty());

                taker.fill(tradedQty);
                makerNode.fill(tradedQty);

                TradeEvent tradeEvent = buildTradeEvent(taker, makerNode, bestPrice, tradedQty);
                accountService.settleTrade(tradeEvent);

                log.info("TRADE: taker={} maker={} amount={} price={}",
                        taker.getOrderId(), makerNode.getOrderId(), tradedQty, bestPrice);

                if (makerNode.isFilled()) {
                    makerBook.removeNode(makerNode);
                    orderMap.remove(makerNode.getOrderId());
                }

                bestLevel = makerBook.getBestLevel();
            }
        }
    }

    private boolean isPriceMatch(Order taker, long bestPrice) {
        if (taker.getSide() == OrderSide.BUY) {
            return taker.getPrice() >= bestPrice;
        }
        return taker.getPrice() <= bestPrice;
    }

    private TradeEvent buildTradeEvent(Order taker, OrderNode makerNode, long tradePrice, long tradedQty) {
        long buyerId = taker.getSide() == OrderSide.BUY ? taker.getUserId() : makerNode.getOrder().getUserId();
        long sellerId = taker.getSide() == OrderSide.SELL ? taker.getUserId() : makerNode.getOrder().getUserId();

        return new TradeEvent(
                makerNode.getOrderId(),
                taker.getOrderId(),
                buyerId,
                sellerId,
                tradePrice,
                tradedQty,
                makerNode.getRemainingQty(),
                taker.getRemainingAmount(),
                makerNode.getStatus(),
                taker.getStatus()
        );
    }

    private void addLimitOrder(Order order) {
        OrderNode node = new OrderNode(order);
        getBook(order.getSide()).addOrder(node);
        orderMap.put(order.getOrderId(), node);
    }

    private OrderBook getBook(OrderSide side) {
        return side == OrderSide.BUY ? bids : asks;
    }

    public Collection<Order> getActiveOrders() {
        if (orderMap == null) {
            return Collections.emptyList();
        }

        List<Order> activeOrders = new ArrayList<>(orderMap.size());
        for (OrderNode node : orderMap.values()) {
            activeOrders.add(node.getOrder());
        }
        return activeOrders;
    }

    public void restoreFromSnapshot(Map<Long, Order> snapshotOrders) {
        log.warn("[Engine] HALT! Replacing memory state with snapshot base...");
        long startTime = System.currentTimeMillis();

        this.bids = new OrderBook(OrderSide.BUY);
        this.asks = new OrderBook(OrderSide.SELL);
        this.orderMap = new HashMap<>();

        for (Order order : snapshotOrders.values()) {
            if (order.getStatus() == null) {
                order.setStatus(order.isCanceled() ? OrderStatus.CANCELED : OrderStatus.NEW);
            }

            if (!order.isCanceled() && !order.isFilled()) {
                addLimitOrder(order);
            }
        }

        long timeTaken = System.currentTimeMillis() - startTime;
        log.warn("[Engine] Snapshot memory injection complete! Rebuilt {} orders in {} ms.", orderMap.size(), timeTaken);
    }

    // [ZH] 只读调试接口，服务于第一阶段行为测试
    // [EN] Read-only inspection helpers used by the phase-1 behavior tests
    public Long getBestBidPrice() {
        return bids.getBestPrice();
    }

    public Long getBestAskPrice() {
        return asks.getBestPrice();
    }

    public Order findActiveOrder(long orderId) {
        OrderNode node = orderMap.get(orderId);
        return node == null ? null : node.getOrder();
    }

    public boolean hasActiveOrder(long orderId) {
        return orderMap.containsKey(orderId);
    }

    public List<Long> getOrderIdsAtPrice(OrderSide side, long price) {
        return getBook(side).getOrderIdsAtPrice(price);
    }

    public int getActiveOrderCount() {
        return orderMap.size();
    }

    public int getBaseCurrency() {
        return baseCurrency;
    }

    public int getQuoteCurrency() {
        return quoteCurrency;
    }
}
