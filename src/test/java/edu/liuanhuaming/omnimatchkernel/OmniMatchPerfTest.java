package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import cn.inlook.cex.infrastructure.journal.CommandJournalEntry;
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

/**
 * [ZH] 撮合引擎极限吞吐量基准测试 (集成 WAL 持久化与快照机制)
 * [EN] Matching Engine Extreme Throughput Benchmark Test (Integrated with WAL and Snapshot)
 */
public class OmniMatchPerfTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchPerfTest.class);

    private static class DummyBalanceManager extends BalanceManager {
        @Override
        public void settle(long buyerId, long sellerId, int baseCurrency, int quoteCurrency, long amount, long price) {
            // [ZH] 压测期间不执行真实的资产计算
            // [EN] No real balance calculation during benchmark
        }
    }

    @Test
    public void testMatchingEngineThroughput() throws InterruptedException {
        log.warn("=====================================================");
        log.warn("[STRESS TEST] Starting benchmark with WAL and Snapshot mechanisms.");
        log.warn("=====================================================");

        MockKafkaBroker.startConsumers();

        BalanceManager dummyBalanceManager = new DummyBalanceManager();
        MatchingEngine engine = new MatchingEngine(dummyBalanceManager);
        MatchingEventHandler handler = new MatchingEventHandler(engine);

        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                1024 * 1024,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
        );

        disruptor.handleEventsWith(handler);
        RingBuffer<OrderEvent> ringBuffer = disruptor.start();

        long totalEvents = 1_000_000L;
        long startTime = System.currentTimeMillis();

        for (long i = 1; i <= totalEvents; i++) {
            // ==========================================
            // 1. [ZH] 正常的业务指令下发 (下单/撤单)
            // [EN] Normal business command dispatch (Place/Cancel)
            // ==========================================
            long sequence = ringBuffer.next();
            try {
                OrderEvent event = ringBuffer.get(sequence);
                Map<String, Object> commandPacket = new HashMap<>();

                if (i % 10 == 0) {
                    CancelOrderCommand cancelOrderCommand = new CancelOrderCommand(i - 1, 0L, System.nanoTime());
                    event.setEventType(DisruptorEventType.CANCEL_ORDER);
                    event.setCancelOrderCommand(cancelOrderCommand);
                    commandPacket.put("type", "CANCEL_ORDER");
                    commandPacket.put("cancelOrderCommand", cancelOrderCommand);
                } else {
                    OrderSide side = (i % 2 == 0) ? OrderSide.BUY : OrderSide.SELL;
                    Order order = new Order(i, 1001L, side, 60000 + (i % 100), 10L);
                    PlaceOrderCommand placeOrderCommand = new PlaceOrderCommand(
                            order.getOrderId(),
                            order.getUserId(),
                            order.getSymbol(),
                            order.getSide(),
                            order.getPrice(),
                            order.getRemainingAmount(),
                            order.getTimestamp());
                    event.setEventType(DisruptorEventType.PLACE_ORDER);
                    event.setPlaceOrderCommand(placeOrderCommand);
                    commandPacket.put("type", "PLACE_ORDER");
                    commandPacket.put("placeOrderCommand", placeOrderCommand);
                }
                MockKafkaBroker.send(commandPacket);
            } finally {
                ringBuffer.publish(sequence);
            }

            // ==========================================
            // 2. [ZH] 🚀 触发点：在第 50 万笔订单时，强行插入快照指令
            // [EN] 🚀 Trigger Point: Forcibly insert a snapshot command at the 500,000th order
            // ==========================================
            if (i == 500_000) {
                log.warn("[STRESS TEST] Reached 500,000 orders. Injecting MAKE_SNAPSHOT command into RingBuffer...");
                long snapSeq = ringBuffer.next();
                try {
                    OrderEvent snapEvent = ringBuffer.get(snapSeq);
                    snapEvent.setEventType(DisruptorEventType.MAKE_SNAPSHOT);

                    // [ZH] 将快照标记同步写入 WAL 日志，用于日后的断点续传
                    // [EN] Synchronously write snapshot marker to WAL for future breakpoint continuation
                    MockKafkaBroker.send(CommandJournalEntry.forSnapshot(snapSeq));
                } finally {
                    ringBuffer.publish(snapSeq);
                }
            }
        }

        // [ZH] 阻塞直至计算核心消费完毕
        // [EN] Block until the matching core finishes consumption
        while (disruptor.getCursor() > handler.getSequence().get()) {
            Thread.yield();
        }

        long endTime = System.currentTimeMillis();

        // [ZH] 执行同步停机，阻塞等待日志线程写完并执行 truncate
        // [EN] Perform synchronous shutdown, wait for log thread and truncate
        MockKafkaBroker.shutdown();
        disruptor.shutdown();

        // [ZH] 确保操作系统完成文件元数据截断
        // [EN] Ensure OS finishes file metadata truncation
        Thread.sleep(100);

        long timeTakenMs = endTime - startTime;
        log.warn("=====================================================");
        log.warn("Benchmark Completed! Time Taken: {} ms", timeTakenMs);

        // [ZH] 注意：由于中间包含了 400ms 左右的纯 I/O 快照时间，整体 TPS 会发生合理范围内的下降
        // [EN] Note: Overall TPS will drop reasonably due to the ~400ms pure I/O snapshot time
        log.warn("Overall Throughput (TPS): {} ops/sec", String.format("%.2f", (totalEvents * 1000.0) / timeTakenMs));
        log.warn("=====================================================");
    }
}
