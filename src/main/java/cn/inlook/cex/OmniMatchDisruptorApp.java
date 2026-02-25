package cn.inlook.cex;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

// [ZH] 高并发完整启动类
// [EN] High-Concurrency Main App - Your GitHub showcase code
@Slf4j
public class OmniMatchDisruptorApp {

    public static void main(String[] args) throws InterruptedException {
        log.info("Booting up OmniMatch-Kernel with LMAX Disruptor...");

        // 1. [ZH] 初始化核心领域服务 / [EN] Initialize core domain services
        BalanceManager balanceManager = new BalanceManager();
        MatchingEngine matchingEngine = new MatchingEngine(balanceManager);

        // [ZH] 注入初始资金 (演示用) / [EN] Inject initial funds (for demo)
        balanceManager.settle(100, 0, 1, 2, 0, 0); // User 100
        balanceManager.settle(200, 0, 1, 2, 0, 0); // User 200

        // 2. [ZH] 配置并启动 Disruptor / [EN] Configure and start Disruptor
        // [ZH] RingBuffer 大小，必须是 2 的 N 次方
        // [EN] RingBuffer size, must be a power of 2
        int bufferSize = 1024 * 1024;

        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                OrderEvent.FACTORY,
                bufferSize,
                DaemonThreadFactory.INSTANCE
        );

        // [ZH] 绑定消费者 (我们的撮合引擎)
        // [EN] Bind the consumer (Our matching engine)
        disruptor.handleEventsWith(new MatchingEventHandler(matchingEngine));
        disruptor.start();

        // 3. [ZH] 获取 RingBuffer (生产者发布事件的入口)
        // [EN] Get RingBuffer (Entry point for producers to publish events)
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();

        log.info("Engine is running. Simulating concurrent order placement...");

        // 4. [ZH] 模拟高并发下单 / [EN] Simulate high-concurrency order placement
        // [ZH] 沈老板 (UID 100) 发布买单
        // [EN] Boss Shen (UID 100) publishes BUY order
        publishOrder(ringBuffer, new Order(1001L, 100L, OrderSide.BUY, 50000L, 10000000L));

        // [ZH] 机器人 (UID 200) 发布卖单
        // [EN] Robot (UID 200) publishes SELL order
        publishOrder(ringBuffer, new Order(1002L, 200L, OrderSide.SELL, 50000L, 10000000L));

        // [ZH] 阻塞主线程一会，让后台消费者跑完
        // [EN] Block main thread slightly to let background consumer finish
        Thread.sleep(1000);
        log.info("Shutting down engine. Good night and have fun with AI Manga!");
        disruptor.shutdown();
    }

    // [ZH] 生产者发布方法：将订单塞入 RingBuffer
    // [EN] Producer publish method: Put order into RingBuffer
    private static void publishOrder(RingBuffer<OrderEvent> ringBuffer, Order order) {
        long sequence = ringBuffer.next();  // [ZH] 申请下一个坑位 / [EN] Claim next slot
        try {
            OrderEvent event = ringBuffer.get(sequence); // [ZH] 获取该坑位的对象 / [EN] Get object at slot
            event.setOrder(order); // [ZH] 填充数据 / [EN] Fill data
        } finally {
            ringBuffer.publish(sequence); // [ZH] 提交发布 (消费者此时才能看到) / [EN] Publish (visible to consumer now)
        }
    }
}