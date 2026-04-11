package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.TradeEvent;

// [ZH] 撮合核心与账务系统之间的最小边界
// [EN] Minimal boundary between matching core and account system
public interface AccountService {

    default boolean reserveForOrder(Order order) {
        return true;
    }

    void settleTrade(TradeEvent tradeEvent);

    default void releaseOnCancel(Order order) {
    }
}
