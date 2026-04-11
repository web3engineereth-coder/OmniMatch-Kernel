package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.TradeEvent;

// [ZH] 撮合结果发布边界
// [EN] Publication boundary for matching results
public interface TradeEventPublisher {

    void publish(TradeEvent tradeEvent);
}
