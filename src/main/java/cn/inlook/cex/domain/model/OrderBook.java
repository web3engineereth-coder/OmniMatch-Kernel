package cn.inlook.cex.domain.model;

import java.util.Collections;
import java.util.LinkedList;
import java.util.TreeMap;

// [ZH] 订单簿 (盘口) 模型
// [EN] Order Book Model
public class OrderBook {

    // [ZH] 盘口方向 (买盘或卖盘)
    // [EN] Side of the book (BUY or SELL)
    private final OrderSide side;

    // [ZH] 核心数据结构：价格 -> 该价格下的订单队列
    // [EN] Core data structure: Price -> Queue of orders at that price
    private final TreeMap<Long, LinkedList<Order>> book;

    public OrderBook(OrderSide side) {
        this.side = side;
        if (side == OrderSide.BUY) {
            // [ZH] 买盘：价格从高到低排序 (降序)
            // [EN] Bids: Sort prices from highest to lowest (Descending)
            this.book = new TreeMap<>(Collections.reverseOrder());
        } else {
            // [ZH] 卖盘：价格从低到高排序 (升序，TreeMap 默认行为)
            // [EN] Asks: Sort prices from lowest to highest (Ascending, TreeMap default)
            this.book = new TreeMap<>();
        }
    }

    // [ZH] 将新订单加入盘口 (挂单)
    // [EN] Add a new order to the book (Maker order)
    public void addOrder(Order order) {
        if (order.getSide() != this.side) {
            throw new IllegalArgumentException("Order side does not match OrderBook side.");
        }
        // [ZH] computeIfAbsent: 如果该价格档位不存在，就创建一个新的 LinkedList
        // [EN] computeIfAbsent: If the price level doesn't exist, create a new LinkedList
        book.computeIfAbsent(order.getPrice(), k -> new LinkedList<>()).add(order);
    }

    // [ZH] 获取当前盘口的最优价格 (买盘最高价，卖盘最低价)
    // [EN] Get the best price currently available in the book
    public Long getBestPrice() {
        return book.isEmpty() ? null : book.firstKey();
    }

    // [ZH] 获取当前最优价格档位的所有订单
    // [EN] Get all orders at the best price level
    public LinkedList<Order> getOrdersAtBestPrice() {
        Long bestPrice = getBestPrice();
        return bestPrice == null ? null : book.get(bestPrice);
    }

    // [ZH] 当某个价格档位的订单全部成交完后，移除该价格档位
    // [EN] Remove the price level when all orders at this price are fully filled
    public void removeBestPrice() {
        if (!book.isEmpty()) {
            book.remove(book.firstKey());
        }
    }

    // [ZH] 判断盘口是否为空
    // [EN] Check if the book is empty
    public boolean isEmpty() {
        return book.isEmpty();
    }
}