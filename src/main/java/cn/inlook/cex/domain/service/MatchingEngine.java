package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.EngineEvent;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderBook;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderNode;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderStatus;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.model.PriceLevel;
import cn.inlook.cex.domain.model.TradeEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * [ZH] 核心撮合引擎 - 单线程事件驱动，内部使用 PriceLevel + OrderNode 结构
 * [EN] Core matching engine using a single-threaded event-driven flow with PriceLevel + OrderNode
 */
@Slf4j
public class MatchingEngine {

    private OrderBook bids;
    private OrderBook asks;
    private final AccountService accountService;
    private final TradeEventPublisher tradeEventPublisher;
    private final OrderCanceledEventPublisher orderCanceledEventPublisher;
    private final OrderRejectedEventPublisher orderRejectedEventPublisher;
    private final EngineEventPublisher engineEventPublisher;
    private final boolean enableInvariantCheck;

    // [ZH] 全局订单节点索引，用于 O(1) 撤单
    // [EN] Global order-node index for O(1) cancellation
    private Map<Long, OrderNode> orderMap = new HashMap<>();

    private final int baseCurrency = 1;
    private final int quoteCurrency = 2;

    public MatchingEngine(BalanceManager balanceManager) {
        this(new BalanceManagerAccountService(balanceManager, 1, 2),
                new NoopTradeEventPublisher(),
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                false);
    }

    public MatchingEngine(AccountService accountService) {
        this(accountService,
                new NoopTradeEventPublisher(),
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                false);
    }

    public MatchingEngine(AccountService accountService, TradeEventPublisher tradeEventPublisher) {
        this(accountService,
                tradeEventPublisher,
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                false);
    }

    public MatchingEngine(AccountService accountService, EngineEventPublisher engineEventPublisher) {
        this(accountService, engineEventPublisher, false);
    }

    public MatchingEngine(AccountService accountService,
                          TradeEventPublisher tradeEventPublisher,
                          boolean enableInvariantCheck) {
        this(accountService,
                tradeEventPublisher,
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                enableInvariantCheck);
    }

    public MatchingEngine(AccountService accountService,
                          EngineEventPublisher engineEventPublisher,
                          boolean enableInvariantCheck) {
        this.bids = new OrderBook(OrderSide.BUY);
        this.asks = new OrderBook(OrderSide.SELL);
        this.accountService = Objects.requireNonNull(accountService, "accountService");
        this.tradeEventPublisher = new NoopTradeEventPublisher();
        this.orderCanceledEventPublisher = new NoopOrderCanceledEventPublisher();
        this.orderRejectedEventPublisher = new NoopOrderRejectedEventPublisher();
        this.engineEventPublisher = Objects.requireNonNull(engineEventPublisher, "engineEventPublisher");
        this.enableInvariantCheck = enableInvariantCheck;
    }

    public MatchingEngine(AccountService accountService,
                          TradeEventPublisher tradeEventPublisher,
                          OrderCanceledEventPublisher orderCanceledEventPublisher,
                          OrderRejectedEventPublisher orderRejectedEventPublisher) {
        this(accountService,
                tradeEventPublisher,
                orderCanceledEventPublisher,
                orderRejectedEventPublisher,
                false);
    }

    public MatchingEngine(AccountService accountService,
                          TradeEventPublisher tradeEventPublisher,
                          OrderCanceledEventPublisher orderCanceledEventPublisher,
                          OrderRejectedEventPublisher orderRejectedEventPublisher,
                          boolean enableInvariantCheck) {
        this.bids = new OrderBook(OrderSide.BUY);
        this.asks = new OrderBook(OrderSide.SELL);
        this.accountService = Objects.requireNonNull(accountService, "accountService");
        this.tradeEventPublisher = Objects.requireNonNull(tradeEventPublisher, "tradeEventPublisher");
        this.orderCanceledEventPublisher = Objects.requireNonNull(orderCanceledEventPublisher, "orderCanceledEventPublisher");
        this.orderRejectedEventPublisher = Objects.requireNonNull(orderRejectedEventPublisher, "orderRejectedEventPublisher");
        this.engineEventPublisher = new DispatchingEngineEventPublisher(
                this.tradeEventPublisher,
                this.orderCanceledEventPublisher,
                this.orderRejectedEventPublisher
        );
        this.enableInvariantCheck = enableInvariantCheck;
    }

    public void handle(PlaceOrderCommand command) {
        Order order = new Order(
                command.getOrderId(),
                command.getUserId(),
                command.getSymbol(),
                command.getSide(),
                command.getPrice(),
                command.getQuantity());
        order.setTimestamp(command.getTimestamp());
        processOrder(order);
    }

    public void handle(CancelOrderCommand command) {
        cancelOrder(command.getOrderId(), command.getSymbol(), command.getUserId(), command.getTimestamp(), true);
    }

    public void processOrder(Order incomingOrder) {
        if (!accountService.reserveForOrder(incomingOrder)) {
            log.warn("Order {} rejected by account boundary.", incomingOrder.getOrderId());
            publishRejectedEvent(
                    incomingOrder.getSymbol(),
                    incomingOrder.getOrderId(),
                    incomingOrder.getUserId(),
                    OrderRejectReason.INSUFFICIENT_BALANCE,
                    incomingOrder.getTimestamp());
            runInvariantCheckIfEnabled();
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

        runInvariantCheckIfEnabled();
    }

    public void cancelOrder(long orderId) {
        cancelOrder(orderId, null, 0L, System.nanoTime(), false);
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

                // [ZH] 成交副作用通过发布边界输出，撮合核心本身不直接记录成交日志
                // [EN] Trade side effects leave through the publication boundary, not direct core logging
                publishEngineEvent(tradeEvent);

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
                taker.getSymbol(),
                makerNode.getOrderId(),
                taker.getOrderId(),
                buyerId,
                sellerId,
                tradePrice,
                tradedQty,
                makerNode.getRemainingQty(),
                taker.getRemainingAmount(),
                makerNode.getStatus(),
                taker.getStatus(),
                System.nanoTime()
        );
    }

    private void addLimitOrder(Order order) {
        OrderNode node = new OrderNode(order);
        getBook(order.getSide()).addOrder(node);
        orderMap.put(order.getOrderId(), node);
    }

    private void cancelOrder(long orderId,
                             String symbol,
                             long userId,
                             long timestamp,
                             boolean publishMissingOrderReject) {
        OrderNode node = orderMap.remove(orderId);
        if (node == null) {
            log.warn("Cancel failed: Order {} not found or already removed.", orderId);
            if (publishMissingOrderReject) {
                publishRejectedEvent(symbol, orderId, userId, OrderRejectReason.ORDER_NOT_FOUND, timestamp);
            }
            runInvariantCheckIfEnabled();
            return;
        }

        Order order = node.getOrder();
        long remainingQuantity = order.getRemainingAmount();
        OrderBook book = getBook(node.getSide());

        // [ZH] 撤单释放必须基于仍然保留的 remainingAmount 计算，因此顺序固定为 release -> cancel -> remove
        // [EN] Cancel release must use the still-intact remainingAmount, so the order is fixed as release -> cancel -> remove
        accountService.releaseOnCancel(order);
        node.cancel();
        book.removeNode(node);
        publishEngineEvent(new OrderCanceledEvent(
                order.getSymbol(),
                order.getOrderId(),
                order.getUserId(),
                remainingQuantity,
                timestamp));
        log.info("Order {} canceled and detached from price level {}.", orderId, node.getPrice());
        runInvariantCheckIfEnabled();
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

    public boolean isInvariantCheckEnabled() {
        return enableInvariantCheck;
    }

    // [ZH] 开发/测试期自检：验证订单簿、价格档位与 orderMap 的关键不变量
    // [EN] Dev/test invariant guard validating the core book, levels, and orderMap consistency
    public void assertInvariant() {
        assertBookInvariant(bids, OrderSide.BUY);
        assertBookInvariant(asks, OrderSide.SELL);
    }

    private void assertBookInvariant(OrderBook book, OrderSide side) {
        Long actualBestPrice = side == OrderSide.BUY ? getBestBidPrice() : getBestAskPrice();
        Long expectedBestPrice = null;
        Set<Long> bookOrderIds = new HashSet<>();

        for (PriceLevel level : book.getLevels()) {
            if (level == null) {
                failInvariant("Encountered null price level on " + side + " side.");
            }
            if (level.isEmpty()) {
                failInvariant("Empty price level must not remain in book: side=" + side + ", price=" + level.getPrice());
            }

            if (expectedBestPrice == null) {
                expectedBestPrice = level.getPrice();
            } else if (side == OrderSide.BUY && level.getPrice() > expectedBestPrice) {
                expectedBestPrice = level.getPrice();
            } else if (side == OrderSide.SELL && level.getPrice() < expectedBestPrice) {
                expectedBestPrice = level.getPrice();
            }

            int traversedSize = 0;
            OrderNode previous = null;
            OrderNode current = level.getHead();
            while (current != null) {
                traversedSize++;
                Order order = current.getOrder();

                if (order == null) {
                    failInvariant("Order node without backing order at price " + level.getPrice());
                }
                if (current.getPriceLevel() != level) {
                    failInvariant("Order node references wrong price level for order " + current.getOrderId());
                }
                if (current.getSide() != side) {
                    failInvariant("Order side does not match book side for order " + current.getOrderId());
                }
                if (current.getPrice() != level.getPrice()) {
                    failInvariant("Order price does not match its price level for order " + current.getOrderId());
                }
                if (current.getPrev() != previous) {
                    failInvariant("Broken previous pointer at order " + current.getOrderId());
                }
                if (order.getRemainingAmount() <= 0 || order.isFilled() || order.isCanceled()) {
                    failInvariant("Inactive order remains in book: orderId=" + current.getOrderId());
                }
                if (!orderMap.containsKey(current.getOrderId())) {
                    failInvariant("Book contains order missing from orderMap: orderId=" + current.getOrderId());
                }
                if (orderMap.get(current.getOrderId()) != current) {
                    failInvariant("orderMap points to a different node instance for order " + current.getOrderId());
                }
                if (!bookOrderIds.add(current.getOrderId())) {
                    failInvariant("Duplicate order found in book traversal: orderId=" + current.getOrderId());
                }

                previous = current;
                current = current.getNext();
            }

            if (traversedSize != level.getSize()) {
                failInvariant("Price level size mismatch at price " + level.getPrice() + ": size=" +
                        level.getSize() + ", traversed=" + traversedSize);
            }
            if (traversedSize == 0 || level.getHead() == null || level.getTail() == null) {
                failInvariant("Non-empty price level has broken head/tail pointers at price " + level.getPrice());
            }
            if (level.getHead().getPrev() != null) {
                failInvariant("Price level head must not have prev pointer at price " + level.getPrice());
            }
            if (level.getTail().getNext() != null) {
                failInvariant("Price level tail must not have next pointer at price " + level.getPrice());
            }
        }

        if (!Objects.equals(expectedBestPrice, actualBestPrice)) {
            failInvariant("Best price mismatch on " + side + " side: expected=" +
                    expectedBestPrice + ", actual=" + actualBestPrice);
        }

        for (Map.Entry<Long, OrderNode> entry : orderMap.entrySet()) {
            OrderNode node = entry.getValue();
            if (node.getSide() != side) {
                continue;
            }
            Order order = node.getOrder();
            if (order.getRemainingAmount() <= 0 || order.isFilled() || order.isCanceled()) {
                failInvariant("Inactive order remains in orderMap: orderId=" + entry.getKey());
            }
            if (node.getPriceLevel() == null) {
                failInvariant("orderMap contains dangling node without price level: orderId=" + entry.getKey());
            }
            if (!bookOrderIds.contains(entry.getKey())) {
                failInvariant("orderMap contains order missing from book: orderId=" + entry.getKey());
            }
        }
    }

    private void failInvariant(String message) {
        throw new IllegalStateException("MatchingEngine invariant violation: " + message);
    }

    private void publishRejectedEvent(String symbol,
                                      long orderId,
                                      long userId,
                                      OrderRejectReason reason,
                                      long timestamp) {
        publishEngineEvent(new OrderRejectedEvent(symbol, orderId, userId, reason, timestamp));
    }

    private void publishEngineEvent(EngineEvent event) {
        engineEventPublisher.publish(event);
    }

    private void runInvariantCheckIfEnabled() {
        if (enableInvariantCheck) {
            assertInvariant();
        }
    }
}
