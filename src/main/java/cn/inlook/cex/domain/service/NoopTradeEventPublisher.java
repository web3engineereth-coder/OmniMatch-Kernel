package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.TradeEvent;

// [ZH] 默认空实现，确保撮合核心没有额外副作用
// [EN] Default no-op implementation to keep the matching core side-effect free
public class NoopTradeEventPublisher implements TradeEventPublisher {

    @Override
    public void publish(TradeEvent tradeEvent) {
        // [ZH] Intentionally no-op
        // [EN] Intentionally no-op
    }
}
