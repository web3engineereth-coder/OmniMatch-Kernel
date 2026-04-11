package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.TradeEvent;
import lombok.extern.slf4j.Slf4j;

// [ZH] 基于日志的最小发布器，承接从撮合核心中移出的输出逻辑
// [EN] Minimal logging publisher that carries output logic out of the matching core
@Slf4j
public class LoggingTradeEventPublisher implements TradeEventPublisher {

    @Override
    public void publish(TradeEvent tradeEvent) {
        log.info("TRADE: taker={} maker={} amount={} price={}",
                tradeEvent.getTakerOrderId(),
                tradeEvent.getMakerOrderId(),
                tradeEvent.getQuantity(),
                tradeEvent.getPrice());
    }
}
