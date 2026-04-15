package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.OrderCanceledEvent;
import cn.inlook.cex.domain.model.OrderRejectedEvent;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.OrderCanceledEventPublisher;
import cn.inlook.cex.domain.service.OrderRejectedEventPublisher;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import cn.inlook.cex.domain.service.SnapshotManager;
import cn.inlook.cex.infrastructure.disruptor.DefaultCommandRouter;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEventHandlerCommandTest {

    @Test
    void shouldRoutePlaceOrderCommandThroughMatchingEventHandler() throws Exception {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 1_000L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);
        MatchingEventHandler handler = new MatchingEventHandler(new DefaultCommandRouter(engine, new SnapshotManager()));
        OrderEvent event = new OrderEvent();
        event.setEventType(DisruptorEventType.PLACE_ORDER);
        event.setPlaceOrderCommand(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 11L));

        handler.onEvent(event, 7L, true);

        assertTrue(engine.hasActiveOrder(1L));
        assertEquals(100L, engine.getBestBidPrice());
        assertEquals(500L, accountService.getAccount(1001L).getFrozenCash());
        assertNull(event.getPlaceOrderCommand());
        assertNull(event.getOrder());
        assertNull(event.getEventType());
        assertTrue(tradePublisher.events.isEmpty());
        assertTrue(canceledPublisher.events.isEmpty());
        assertTrue(rejectedPublisher.events.isEmpty());
    }

    @Test
    void shouldRouteCancelOrderCommandThroughMatchingEventHandler() throws Exception {
        InMemoryAccountService accountService = new InMemoryAccountService();
        accountService.createAccount(1001L, 1_000L, 0L);
        RecordingTradeEventPublisher tradePublisher = new RecordingTradeEventPublisher();
        RecordingOrderCanceledEventPublisher canceledPublisher = new RecordingOrderCanceledEventPublisher();
        RecordingOrderRejectedEventPublisher rejectedPublisher = new RecordingOrderRejectedEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, tradePublisher, canceledPublisher, rejectedPublisher);
        MatchingEventHandler handler = new MatchingEventHandler(new DefaultCommandRouter(engine, new SnapshotManager()));

        OrderEvent placeEvent = new OrderEvent();
        placeEvent.setEventType(DisruptorEventType.PLACE_ORDER);
        placeEvent.setPlaceOrderCommand(new PlaceOrderCommand(1L, 1001L, "BTC-USDT", OrderSide.BUY, 100L, 5L, 11L));
        handler.onEvent(placeEvent, 1L, true);

        OrderEvent cancelEvent = new OrderEvent();
        cancelEvent.setEventType(DisruptorEventType.CANCEL_ORDER);
        cancelEvent.setCancelOrderCommand(new CancelOrderCommand("BTC-USDT", 1L, 1001L, 12L));
        handler.onEvent(cancelEvent, 2L, true);

        assertFalse(engine.hasActiveOrder(1L));
        assertNull(engine.getBestBidPrice());
        assertEquals(0L, accountService.getAccount(1001L).getFrozenCash());
        assertEquals(1_000L, accountService.getAccount(1001L).getAvailableCash());
        assertEquals(1, canceledPublisher.events.size());
        assertEquals(1L, canceledPublisher.events.get(0).getOrderId());
        assertEquals(5L, canceledPublisher.events.get(0).getRemainingQuantity());
        assertNull(cancelEvent.getCancelOrderCommand());
        assertNull(cancelEvent.getEventType());
        assertTrue(rejectedPublisher.events.isEmpty());
    }

    private static class RecordingTradeEventPublisher implements TradeEventPublisher {
        private final List<TradeEvent> events = new ArrayList<>();

        @Override
        public void publish(TradeEvent tradeEvent) {
            events.add(tradeEvent);
        }
    }

    private static class RecordingOrderCanceledEventPublisher implements OrderCanceledEventPublisher {
        private final List<OrderCanceledEvent> events = new ArrayList<>();

        @Override
        public void publish(OrderCanceledEvent event) {
            events.add(event);
        }
    }

    private static class RecordingOrderRejectedEventPublisher implements OrderRejectedEventPublisher {
        private final List<OrderRejectedEvent> events = new ArrayList<>();

        @Override
        public void publish(OrderRejectedEvent event) {
            events.add(event);
        }
    }
}
