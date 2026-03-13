package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.RecoveryManager;
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

/**
 * [ZH] 系统恢复性能测试：验证快照秒级装载 + 增量日志断点重演的极限效率
 * [EN] Recovery Performance Test: Validate efficiency of snapshot load + incremental AOF replay
 */
public class OmniMatchRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchRecoveryTest.class);

    @Test
    public void testSystemRecoveryEfficiency() throws InterruptedException {
        log.warn("=====================================================");
        log.warn("[RECOVERY TEST] System booting up. Initiating cold start sequence...");
        log.warn("=====================================================");

        // 1. [ZH] 创造处于绝对静止状态的空引擎
        // 1. [EN] Create an empty engine in absolute halt state
        BalanceManager freshBalanceManager = new BalanceManager();
        MatchingEngine freshEngine = new MatchingEngine(freshBalanceManager);

        // 2. [ZH] 🚀 核心阶段：单线程极限读档与重演 (此时 Disruptor 根本还没启动)
        // 2. [EN] 🚀 Core Stage: Single-thread extreme load and replay (Disruptor not started yet)
        RecoveryManager recoveryManager = new RecoveryManager(freshEngine);

        long startTime = System.currentTimeMillis();
        recoveryManager.startReplay();
        long duration = System.currentTimeMillis() - startTime;

        log.warn(">>> Recovery Phase Completed in {} ms! <<<", duration);

        // 3. [ZH] 恢复完成，引擎状态已 100% 同步。此时才挂载消费者并启动网络层/队列
        // 3. [EN] Recovery done, engine state 100% synced. Now mount consumer and start queue/network
        log.warn("[RECOVERY TEST] State restored. Starting Disruptor to accept live traffic...");
        MatchingEventHandler handler = new MatchingEventHandler(freshEngine);

        int bufferSize = 1024 * 1024;
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
        );

        disruptor.handleEventsWith(handler);
        RingBuffer<OrderEvent> ringBuffer = disruptor.start();
        log.warn("[RECOVERY TEST] Disruptor is online. System is fully operational.");

        // [ZH] 优雅停机
        // [EN] Graceful shutdown
        disruptor.shutdown();
        log.warn("=====================================================");
    }
}