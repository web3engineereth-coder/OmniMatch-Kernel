package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

// [ZH] 撮合引擎极限吞吐量基准测试 (JUnit)
// [EN] Matching Engine Extreme Throughput Benchmark Test (JUnit)
public class OmniMatchPerfTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchPerfTest.class);

    // [ZH] 模拟一个空的资产管理器，防止压测时因空指针报错
    // [EN] Mock an empty BalanceManager to prevent NullPointerException during benchmark
    private static class DummyBalanceManager extends BalanceManager {
        @Override
        public void settle(long buyerId, long sellerId, int baseCurrency, int quoteCurrency, long amount, long price) {
            // [ZH] 压测期间不执行真实的资产计算，仅测试 CPU 撮合与路由极限
            // [EN] Do not execute real balance calculation during benchmark, only test CPU matching and routing limits
        }
    }

    @Test
    public void testMatchingEngineThroughput() throws InterruptedException {
        log.warn("=====================================================");
        log.warn("[WARN] Starting high-frequency performance benchmark.");
        log.warn("[WARN] 极其重要: 为了获得真实的 TPS，请确保 logback.xml 中日志级别为 WARN 或 ERROR，或注释掉 MatchingEngine 中的 log.info！");
        log.warn("=====================================================");

        BalanceManager dummyBalanceManager = new DummyBalanceManager();
        MatchingEngine engine = new MatchingEngine(dummyBalanceManager);
        MatchingEventHandler handler = new MatchingEventHandler(engine);

        int bufferSize = 1024 * 1024;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                bufferSize,
                threadFactory,
                ProducerType.SINGLE, // [ZH] 单生产者模式，追求极限写入 / [EN] Single producer mode for extreme writing
                new YieldingWaitStrategy() // [ZH] 让出 CPU 策略，极低延迟 / [EN] Yielding strategy for ultra-low latency
        );

        disruptor.handleEventsWith(handler);
        RingBuffer<OrderEvent> ringBuffer = disruptor.start();

        long totalEvents = 1_000_000L; // [ZH] 100 万笔事件 / [EN] 1 Million events
        long placeCount = 0;
        long cancelCount = 0;

        log.warn("Engine started. Injecting {} events...", totalEvents);

        long startTime = System.currentTimeMillis();

        // [ZH] 疯狂灌入 100 万单
        for (long i = 1; i <= totalEvents; i++) {
            long sequence = ringBuffer.next();
            try {
                OrderEvent event = ringBuffer.get(sequence);

                if (i % 10 == 0) {
                    event.setEventType(DisruptorEventType.CANCEL_ORDER);
                    event.setCancelOrderId(i - 1);
                    cancelCount++;
                } else {
                    OrderSide side = (i % 2 == 0) ? OrderSide.BUY : OrderSide.SELL;
                    long price = 60000 + (i % 100);
                    // [ZH] 参数: orderId, userId, side, price, amount
                    Order order = new Order(i, 1001L, side, price, 10L);

                    event.setEventType(DisruptorEventType.PLACE_ORDER);
                    event.setOrder(order);
                    placeCount++;
                }
            } finally {
                ringBuffer.publish(sequence);
            }
        }

        // [ZH] 等待消费者处理完毕 (Disruptor 异步处理，需确保消费指针追上生产指针)
        // [EN] Wait for consumer to finish (Ensure consumer cursor catches up with producer cursor)
        while (disruptor.getCursor() > handler.getSequence().get()) {
            Thread.yield();
        }

        long endTime = System.currentTimeMillis();
        disruptor.shutdown();

        long timeTakenMs = endTime - startTime;
        double tps = (totalEvents * 1000.0) / timeTakenMs;

        log.warn("=====================================================");
        log.warn("Benchmark Completed!");
        log.warn("Total Events Processed: {} (Placed: {}, Canceled: {})", totalEvents, placeCount, cancelCount);
        log.warn("Time Taken: {} ms", timeTakenMs);
        log.warn("Throughput (TPS): {} ops/sec", String.format("%.2f", tps));
        log.warn("=====================================================");
    }
}