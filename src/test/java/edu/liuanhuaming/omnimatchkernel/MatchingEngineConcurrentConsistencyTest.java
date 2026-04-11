package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.TradeEvent;
import cn.inlook.cex.domain.service.AccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.TradeEventPublisher;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineConcurrentConsistencyTest {

    @Test
    void shouldRemainConsistentWithMultiThreadProducersAndSingleThreadMatcher() throws Exception {
        RecordingAccountService accountService = new RecordingAccountService();
        RecordingTradeEventPublisher publisher = new RecordingTradeEventPublisher();
        MatchingEngine engine = new MatchingEngine(accountService, publisher);
        MatchingEventHandler handler = new MatchingEventHandler(engine);

        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                1024 * 64,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new YieldingWaitStrategy()
        );
        disruptor.handleEventsWith(handler);
        RingBuffer<OrderEvent> ringBuffer = disruptor.start();

        try {
            ExecutorService producers = Executors.newFixedThreadPool(4);

            publishSellRangeConcurrently(producers, ringBuffer, 1L, 120L, 100L);
            waitUntilConsumed(disruptor, handler);

            assertEquals(120, engine.getActiveOrderCount());
            assertEquals(100L, engine.getBestAskPrice());
            assertEquals(120, engine.getOrderIdsAtPrice(OrderSide.SELL, 100L).size());

            publishBuyRangeConcurrently(producers, ringBuffer, 1001L, 1070L, 100L);
            waitUntilConsumed(disruptor, handler);

            assertEquals(50, engine.getActiveOrderCount());
            assertEquals(100L, engine.getBestAskPrice());
            assertNull(engine.getBestBidPrice());
            assertEquals(50, engine.getOrderIdsAtPrice(OrderSide.SELL, 100L).size());
            assertEquals(70, publisher.tradeEvents.size());

            List<Long> remainingOrderIds = new ArrayList<>(engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
            List<Long> cancelTargets = remainingOrderIds.subList(0, 20);
            publishCancelsConcurrently(producers, ringBuffer, cancelTargets);
            waitUntilConsumed(disruptor, handler);

            assertEquals(30, engine.getActiveOrderCount());
            assertEquals(30, engine.getOrderIdsAtPrice(OrderSide.SELL, 100L).size());
            for (long orderId : cancelTargets) {
                assertFalse(engine.hasActiveOrder(orderId));
            }

            publishBuyRangeConcurrently(producers, ringBuffer, 2001L, 2030L, 100L);
            waitUntilConsumed(disruptor, handler);

            assertEquals(0, engine.getActiveOrderCount());
            assertEquals(List.of(), engine.getOrderIdsAtPrice(OrderSide.SELL, 100L));
            assertNull(engine.getBestAskPrice());
            assertNull(engine.getBestBidPrice());
            assertEquals(100, publisher.tradeEvents.size());

            producers.shutdownNow();
        } finally {
            disruptor.shutdown();
        }
    }

    private void publishSellRangeConcurrently(ExecutorService producers, RingBuffer<OrderEvent> ringBuffer,
                                              long startId, long endId, long price) throws InterruptedException {
        publishRangeConcurrently(producers, 4, startId, endId, orderId ->
                publishPlace(ringBuffer, new Order(orderId, 5000L + orderId, OrderSide.SELL, price, 1L)));
    }

    private void publishBuyRangeConcurrently(ExecutorService producers, RingBuffer<OrderEvent> ringBuffer,
                                             long startId, long endId, long price) throws InterruptedException {
        publishRangeConcurrently(producers, 4, startId, endId, orderId ->
                publishPlace(ringBuffer, new Order(orderId, 8000L + orderId, OrderSide.BUY, price, 1L)));
    }

    private void publishCancelsConcurrently(ExecutorService producers, RingBuffer<OrderEvent> ringBuffer,
                                            List<Long> cancelTargets) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(cancelTargets.size());
        for (long orderId : cancelTargets) {
            producers.submit(() -> {
                try {
                    publishCancel(ringBuffer, orderId);
                } finally {
                    done.countDown();
                }
            });
        }
        done.await();
    }

    private void publishRangeConcurrently(ExecutorService producers, int threadCount, long startId, long endId,
                                          LongPublisher publisher) throws InterruptedException {
        long total = endId - startId + 1;
        long chunk = total / threadCount;
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final long from = startId + (i * chunk);
            final long to = (i == threadCount - 1) ? endId : (from + chunk - 1);
            producers.submit(() -> {
                try {
                    for (long orderId = from; orderId <= to; orderId++) {
                        publisher.publish(orderId);
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        done.await();
    }

    private void publishPlace(RingBuffer<OrderEvent> ringBuffer, Order order) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setEventType(DisruptorEventType.PLACE_ORDER);
            event.setOrder(order);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void publishCancel(RingBuffer<OrderEvent> ringBuffer, long orderId) {
        long sequence = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(sequence);
            event.setEventType(DisruptorEventType.CANCEL_ORDER);
            event.setCancelOrderId(orderId);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void waitUntilConsumed(Disruptor<OrderEvent> disruptor, MatchingEventHandler handler) {
        while (disruptor.getCursor() > handler.getSequence().get()) {
            Thread.yield();
        }
    }

    @FunctionalInterface
    private interface LongPublisher {
        void publish(long value);
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
