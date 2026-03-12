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
 * [ZH] 系统恢复性能测试：验证从 AOF 日志重演 1,000,000 指令的效率
 * [EN] Recovery Performance Test: Validate efficiency of replaying 1,000,000 commands from AOF
 */
public class OmniMatchRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchRecoveryTest.class);

    @Test
    public void testSystemRecoveryEfficiency() throws InterruptedException {
        log.warn("=====================================================");
        log.warn("[RECOVERY TEST] Initializing fresh engine for state replay...");
        log.warn("=====================================================");

        // 1. [ZH] 初始化全新的空引擎实例 / [EN] Initialize a fresh empty engine instance
        BalanceManager freshBalanceManager = new BalanceManager();
        MatchingEngine freshEngine = new MatchingEngine(freshBalanceManager);
        MatchingEventHandler handler = new MatchingEventHandler(freshEngine);

        // 2. [ZH] 启动 Disruptor 管道 / [EN] Start Disruptor pipeline
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

        // 3. [ZH] 执行重演逻辑 / [EN] Execute replay logic
        RecoveryManager recoveryManager = new RecoveryManager();

        log.warn(">>> Start replaying from journal file...");
        long startTime = System.currentTimeMillis();

        // [ZH] 调用 RecoveryManager 逐行解析并注入 Disruptor
        // [EN] Invoke RecoveryManager to parse and inject into Disruptor line by line
        recoveryManager.startReplay(ringBuffer);

        // 4. [ZH] 等待消费者完成最后一笔指令处理 / [EN] Wait for consumer to finish last command
        long lastSeq = ringBuffer.getCursor();
        while (handler.getSequence().get() < lastSeq) {
            Thread.yield();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // 5. [ZH] 结果输出 / [EN] Output results
        log.warn("=====================================================");
        log.warn("Recovery Completed!");
        log.warn("Time Taken for Replay: {} ms", duration);
        if (duration > 0) {
            double replayTps = (1000000.0 * 1000) / duration;
            log.warn("Replay Speed: {} ops/sec", String.format("%.2f", replayTps));
        }
        log.warn("=====================================================");

        disruptor.shutdown();
    }
}