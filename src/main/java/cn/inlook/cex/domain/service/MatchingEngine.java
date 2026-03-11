package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderBook;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.infrastructure.mq.MockKafkaBroker; // [ZH] 引入模拟 Kafka / [EN] Import mock Kafka
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * [ZH] 核心撮合引擎 - 集成账务结算、异步广播与 O(1) 极速撤单
 * [EN] Core Matching Engine - Integrated with Balance Settlement, Async Broadcast, and O(1) Cancel
 */
@Slf4j
public class MatchingEngine {

    private final OrderBook bids;
    private final OrderBook asks;
    private final BalanceManager balanceManager; // [ZH] 引入账务管理器 / [EN] Inject BalanceManager

    // ==========================================
    // [ZH] 🚀 核心武器：全局订单哈希索引，实现 O(1) 撤单定位
    // [EN] 🚀 Core Weapon: Global order hash index for O(1) cancel positioning
    // ==========================================
    private final Map<Long, Order> orderIndex = new HashMap<>();

    // [ZH] 假设目前系统只处理一对交易对，比如 BTC/USDT
    // [EN] Assume the system handles one trading pair, e.g., BTC/USDT
    private final int baseCurrency = 1;  // BTC
    private final int quoteCurrency = 2; // USDT

    public MatchingEngine(BalanceManager balanceManager) {
        this.bids = new OrderBook(OrderSide.BUY);
        this.asks = new OrderBook(OrderSide.SELL);
        this.balanceManager = balanceManager;
    }

    public void processOrder(Order takerOrder) {
        if (takerOrder.getSide() == OrderSide.BUY) {
            match(takerOrder, asks);
        } else {
            match(takerOrder, bids);
        }

        if (!takerOrder.isFilled()) {
            addLimitOrder(takerOrder);
        }
    }

    // ==========================================
    // [ZH] 新增：O(1) 极限撤单逻辑 (墓碑模式)
    // [EN] New: O(1) Limit-speed Cancellation Logic (Tombstone Pattern)
    // ==========================================
    public void cancelOrder(long orderId) {
        // [ZH] 1. O(1) 极速定位订单实体
        // [EN] 1. O(1) ultra-fast positioning of the order entity
        Order orderToCancel = orderIndex.get(orderId);

        if (orderToCancel == null) {
            log.warn("Cancel failed: Order {} not found or already filled.", orderId);
            return;
        }

        // [ZH] 2. 打上墓碑标记，逻辑删除 (需确保 Order 对象内将其 remainingAmount 归零)
        // [EN] 2. Mark with tombstone, logical deletion (Ensure Order object sets remainingAmount to zero)
        orderToCancel.cancel();

        // [ZH] 3. 从全局索引中立即剔除，释放内存追踪
        // [EN] 3. Remove from global index immediately to release memory tracking
        orderIndex.remove(orderId);

        log.info("Order {} marked as CANCELED (Tombstone applied).", orderId);
    }

    private void match(Order taker, OrderBook makerBook) {
        while (!makerBook.isEmpty() && !taker.isFilled()) {
            Long bestPrice = makerBook.getBestPrice();

            if (taker.getSide() == OrderSide.BUY && taker.getPrice() < bestPrice) break;
            if (taker.getSide() == OrderSide.SELL && taker.getPrice() > bestPrice) break;

            LinkedList<Order> ordersAtPrice = makerBook.getOrdersAtBestPrice();

            while (ordersAtPrice != null && !ordersAtPrice.isEmpty() && !taker.isFilled()) {
                Order maker = ordersAtPrice.peek();

                // ==========================================
                // [ZH] 🚀 墓碑清理：遇到已被撤销的僵尸订单，直接弹出并跳过
                // [EN] 🚀 Tombstone Cleanup: Poll and skip canceled zombie orders
                // ==========================================
                if (maker.isCanceled()) {
                    ordersAtPrice.poll();
                    continue;
                }

                long tradedAmount = Math.min(taker.getRemainingAmount(), maker.getRemainingAmount());

                // 1. [ZH] 执行内存状态扣减 / [EN] Execute memory state deduction
                taker.fill(tradedAmount);
                maker.fill(tradedAmount);

                // 2. [ZH] 调用账务系统进行结算 / [EN] Call BalanceManager for settlement
                // [ZH] 核心：买家和卖家的角色由 taker/maker 的 Side 决定
                // [EN] Core: Buyer/Seller roles determined by taker/maker Side
                long buyerId = (taker.getSide() == OrderSide.BUY) ? taker.getUserId() : maker.getUserId();
                long sellerId = (taker.getSide() == OrderSide.SELL) ? taker.getUserId() : maker.getUserId();

                balanceManager.settle(buyerId, sellerId, baseCurrency, quoteCurrency, tradedAmount, bestPrice);

                // 3. [ZH] 架构核心：结算成功后，异步广播成交结果！(绝不能在这里同步写库)
                //    [EN] Core Arch: Async broadcast after settlement! (NEVER sync write to DB here)
                String tradeRecord = String.format(
                        "{\"buyerUid\": %d, \"sellerUid\": %d, \"price\": %d, \"amount\": %d}",
                        buyerId, sellerId, bestPrice, tradedAmount
                );
                MockKafkaBroker.send(tradeRecord); // [ZH] 极速发送并返回 / [EN] Fire and forget

                // 4. [ZH] 日志记录（生产环境应使用异步 Logger） / [EN] Logging (use async logger in production)
                log.info("TRADE: {} matched with {}, Amount: {}, Price: {}",
                        taker.getOrderId(), maker.getOrderId(), tradedAmount, bestPrice);

                if (maker.isFilled()) {
                    ordersAtPrice.poll();
                    // [ZH] 极其重要：订单完全成交后，必须从全局哈希索引中移除，防止内存泄漏 (OOM)
                    // [EN] Crucial: Remove from global hash index after full fill to prevent memory leak (OOM)
                    orderIndex.remove(maker.getOrderId());
                }
            }

            if (ordersAtPrice == null || ordersAtPrice.isEmpty()) {
                makerBook.removeBestPrice();
            }
        }
    }

    private void addLimitOrder(Order order) {
        if (order.getSide() == OrderSide.BUY) {
            bids.addOrder(order);
        } else {
            asks.addOrder(order);
        }
        // [ZH] 挂单存入全局哈希表，用于后续 O(1) 撤单定位
        // [EN] Store maker order in global hash map for subsequent O(1) cancel positioning
        orderIndex.put(order.getOrderId(), order);
    }
}