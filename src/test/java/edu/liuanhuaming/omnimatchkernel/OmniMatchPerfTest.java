package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import cn.inlook.cex.infrastructure.mq.MockKafkaBroker;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * [ZH] 撮合引擎极限吞吐量基准测试 (集成 WAL 持久化)
 * [EN] Matching Engine Extreme Throughput Benchmark Test (Integrated with WAL Persistence)
 */
public class OmniMatchPerfTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchPerfTest.class);

    private static class DummyBalanceManager extends BalanceManager {
        @Override
        public void settle(long buyerId, long sellerId, int baseCurrency, int quoteCurrency, long amount, long price) {
            // [ZH] 压测期间不执行真实的资产计算 / [EN] No real balance calculation during benchmark
        }
    }

    @Test
    public void testMatchingEngineThroughput() throws InterruptedException {
        log.warn("=====================================================");
        log.warn("[STRESS TEST] Starting benchmark with WAL persistence.");
        log.warn("=====================================================");

        // [ZH] 1. 初始化持久化线程 / [EN] Initialize persistence thread
        MockKafkaBroker.startConsumers();

        BalanceManager dummyBalanceManager = new DummyBalanceManager();
        MatchingEngine engine = new MatchingEngine(dummyBalanceManager);
        MatchingEventHandler handler = new MatchingEventHandler(engine);

        int bufferSize = 1024 * 1024;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                bufferSize,
                threadFactory,
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
        );

        disruptor.handleEventsWith(handler);
        RingBuffer<OrderEvent> ringBuffer = disruptor.start();

        long totalEvents = 1_000_000L;
        log.warn("Injecting {} events with WAL recording...", totalEvents);

        long startTime = System.currentTimeMillis();

        for (long i = 1; i <= totalEvents; i++) {
            long sequence = ringBuffer.next();
            try {
                OrderEvent event = ringBuffer.get(sequence);

                // [ZH] 构造指令包用于 WAL 落盘
                // [EN] Construct command packet for WAL logging
                Map<String, Object> commandPacket = new HashMap<>();

                if (i % 10 == 0) {
                    event.setEventType(DisruptorEventType.CANCEL_ORDER);
                    event.setCancelOrderId(i - 1);

                    commandPacket.put("type", "CANCEL_ORDER");
                    commandPacket.put("data", i - 1);
                } else {
                    OrderSide side = (i % 2 == 0) ? OrderSide.BUY : OrderSide.SELL;
                    long price = 60000 + (i % 100);
                    Order order = new Order(i, 1001L, side, price, 10L);

                    event.setEventType(DisruptorEventType.PLACE_ORDER);
                    event.setOrder(order);

                    commandPacket.put("type", "PLACE_ORDER");
                    commandPacket.put("data", order);
                }

                // ==========================================
                // [ZH] 🚀 核心改动：在发布到 Disruptor 之前，先提交给持久化组件 (WAL)
                // [ZH] 这样磁盘上存入的是原始指令，系统崩溃后可完全恢复
                // [EN] 🚀 Core Change: Submit to persistence (WAL) BEFORE publishing to Disruptor
                // [EN] This ensures raw commands are logged for full recovery after crash
                // ==========================================
                MockKafkaBroker.send(commandPacket);

            } finally {
                ringBuffer.publish(sequence);
            }
        }

        // [ZH] 等待消费者处理完毕 / [EN] Wait for consumer to finish
        while (disruptor.getCursor() > handler.getSequence().get()) {
            Thread.yield();
        }

        long endTime = System.currentTimeMillis();

        // [ZH] 停止持久化并确保所有数据已刷盘
        // [EN] Shutdown persistence and ensure all data is flushed to disk
        MockKafkaBroker.shutdown();
        disruptor.shutdown();

        long timeTakenMs = endTime - startTime;
        log.warn("Throughput (TPS): {} ops/sec", String.format("%.2f", (totalEvents * 1000.0) / timeTakenMs));
        log.warn("=====================================================");
    }
}