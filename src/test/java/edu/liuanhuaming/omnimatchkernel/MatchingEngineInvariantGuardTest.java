package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.AccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineInvariantGuardTest {

    @Test
    void shouldExposeInvariantFlagState() {
        MatchingEngine defaultEngine = new MatchingEngine(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher()
        );
        MatchingEngine guardedEngine = new MatchingEngine(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher(),
                true
        );

        assertFalse(defaultEngine.isInvariantCheckEnabled());
        assertTrue(guardedEngine.isInvariantCheckEnabled());
    }

    @Test
    void shouldPassInvariantForMultiLevelBooks() {
        MatchingEngine engine = new MatchingEngine(new RecordingAccountService(), new RecordingTradeEventPublisher());

        engine.processOrder(new Order(1L, 101L, OrderSide.BUY, 99L, 5L));
        engine.processOrder(new Order(2L, 102L, OrderSide.BUY, 101L, 5L));
        engine.processOrder(new Order(3L, 103L, OrderSide.BUY, 100L, 5L));
        engine.processOrder(new Order(4L, 201L, OrderSide.SELL, 105L, 5L));
        engine.processOrder(new Order(5L, 202L, OrderSide.SELL, 103L, 5L));
        engine.processOrder(new Order(6L, 203L, OrderSide.SELL, 104L, 5L));

        engine.assertInvariant();
    }

    @Test
    void shouldRunInvariantGuardAutomaticallyWhenEnabled() {
        MatchingEngine engine = new MatchingEngine(
                new RecordingAccountService(),
                new RecordingTradeEventPublisher(),
                true
        );

        engine.processOrder(new Order(1L, 201L, OrderSide.SELL, 100L, 10L));
        engine.processOrder(new Order(2L, 101L, OrderSide.BUY, 100L, 5L));
        engine.cancelOrder(1L);

        engine.assertInvariant();
    }

    @Test
    void shouldPassInvariantAfterPartialFillAndCancel() {
        MatchingEngine engine = new MatchingEngine(new RecordingAccountService(), new RecordingTradeEventPublisher());

        engine.processOrder(new Order(1L, 201L, OrderSide.SELL, 100L, 10L));
        engine.processOrder(new Order(2L, 101L, OrderSide.BUY, 100L, 5L));
        engine.assertInvariant();

        engine.cancelOrder(1L);
        engine.assertInvariant();
    }

    @Test
    void shouldPassInvariantAfterFullFillRemovesLevel() {
        MatchingEngine engine = new MatchingEngine(new RecordingAccountService(), new RecordingTradeEventPublisher());

        engine.processOrder(new Order(1L, 201L, OrderSide.SELL, 100L, 5L));
        engine.processOrder(new Order(2L, 101L, OrderSide.BUY, 100L, 5L));

        engine.assertInvariant();
    }

    @Test
    void shouldPassInvariantAfterBatchAndConcurrentStyleStateTransitions() {
        MatchingEngine engine = new MatchingEngine(new RecordingAccountService(), new RecordingTradeEventPublisher());

        for (long orderId = 1; orderId <= 20; orderId++) {
            engine.processOrder(new Order(orderId, 5000L + orderId, OrderSide.BUY, 101L, 1L));
        }
        for (long orderId = 21; orderId <= 40; orderId++) {
            engine.processOrder(new Order(orderId, 6000L + orderId, OrderSide.SELL, 105L, 1L));
        }
        engine.assertInvariant();

        for (long orderId = 1001; orderId <= 1010; orderId++) {
            engine.processOrder(new Order(orderId, 7000L + orderId, OrderSide.SELL, 101L, 1L));
        }
        for (long orderId = 1; orderId <= 5; orderId++) {
            engine.cancelOrder(orderId);
        }
        engine.assertInvariant();

        for (long orderId = 2001; orderId <= 2008; orderId++) {
            engine.processOrder(new Order(orderId, 8000L + orderId, OrderSide.BUY, 105L, 1L));
        }
        engine.assertInvariant();
    }

    private static class RecordingAccountService implements AccountService {
        private final List<TradeEvent> tradeEvents = new ArrayList<>();

        @Override
        public void settleTrade(TradeEvent tradeEvent) {
            tradeEvents.add(tradeEvent);
        }
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> tradeEvents = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            tradeEvents.add(tradeEvent);
        }
    }
}
