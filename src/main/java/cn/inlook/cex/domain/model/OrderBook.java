package cn.inlook.cex.domain.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

// [ZH] 订单簿 (盘口) 模型
// [EN] Order Book Model
public class OrderBook {

    // [ZH] 盘口方向 (买盘或卖盘)
    // [EN] Side of the book (BUY or SELL)
    private final OrderSide side;

    // [ZH] 核心数据结构：价格 -> 价格档位
    // [EN] Core data structure: Price -> Price Level
    private final TreeMap<Long, PriceLevel> levels;

    public OrderBook(OrderSide side) {
        this.side = side;
        if (side == OrderSide.BUY) {
            // [ZH] 买盘：价格从高到低排序 (降序)
            // [EN] Bids: Sort prices from highest to lowest (Descending)
            this.levels = new TreeMap<>(Collections.reverseOrder());
        } else {
            // [ZH] 卖盘：价格从低到高排序 (升序，TreeMap 默认行为)
            // [EN] Asks: Sort prices from lowest to highest (Ascending, TreeMap default)
            this.levels = new TreeMap<>();
        }
    }

    // [ZH] 将新挂单节点追加到价格档尾部，保持 FIFO
    // [EN] Append a new resting node to the end of the price level to keep FIFO
    public void addOrder(OrderNode node) {
        if (node.getSide() != this.side) {
            throw new IllegalArgumentException("Order side does not match OrderBook side.");
        }

        PriceLevel level = levels.computeIfAbsent(node.getPrice(), PriceLevel::new);
        level.append(node);
    }

    // [ZH] 获取当前盘口的最优价格 (买盘最高价，卖盘最低价)
    // [EN] Get the best price currently available in the book
    public Long getBestPrice() {
        return levels.isEmpty() ? null : levels.firstKey();
    }

    public PriceLevel getBestLevel() {
        Long bestPrice = getBestPrice();
        return bestPrice == null ? null : levels.get(bestPrice);
    }

    // [ZH] 只读返回当前订单簿中的价格档位，供调试/自检使用
    // [EN] Read-only snapshot of the current price levels for validation/debugging
    public List<PriceLevel> getLevels() {
        return new ArrayList<>(levels.values());
    }

    // [ZH] 通过节点执行 O(1) 物理删除，档位空则移除
    // [EN] Perform O(1) physical removal through the node and drop the level when empty
    public void removeNode(OrderNode node) {
        PriceLevel level = node.getPriceLevel();
        if (level == null) {
            return;
        }

        level.unlink(node);
        if (level.isEmpty()) {
            levels.remove(level.getPrice());
        }
    }

    public List<Long> getOrderIdsAtPrice(long price) {
        PriceLevel level = levels.get(price);
        List<Long> orderIds = new ArrayList<>();
        if (level == null) {
            return orderIds;
        }

        OrderNode current = level.getHead();
        while (current != null) {
            orderIds.add(current.getOrderId());
            current = current.getNext();
        }
        return orderIds;
    }

    // [ZH] 判断盘口是否为空
    // [EN] Check if the book is empty
    public boolean isEmpty() {
        return levels.isEmpty();
    }
}
