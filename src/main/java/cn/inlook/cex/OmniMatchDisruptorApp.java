package cn.inlook.cex;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.BalanceManager;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.infrastructure.disruptor.JournalEventHandler;
import cn.inlook.cex.infrastructure.disruptor.MatchingEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import cn.inlook.cex.infrastructure.journal.DiskJournaler;
import cn.inlook.cex.infrastructure.mq.MockKafkaBroker; // [ZH] 引入模拟 Kafka / [EN] Import mock Kafka
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// [ZH] 高并发完整启动类 (LMAX 金融级升级版 - 多线程压测)
// [EN] High-Concurrency Main App (LMAX Financial Grade Upgraded - Multi-Thread Test)
@Slf4j
public class OmniMatchDisruptorApp {

    public static void main(String[] args) throws InterruptedException {
        log.info("Booting up OmniMatch-Kernel with LMAX Disruptor (Journaling + Async Mock Kafka)...");

        // [ZH] 0. 启动模拟的下游 Kafka 消费者 / [EN] Start mock downstream Kafka consumer
        MockKafkaBroker.startConsumers();

        // 1. [ZH] 初始化核心领域服务与持久化组件 / [EN] Initialize core domain services and persistence
        BalanceManager balanceManager = new BalanceManager();
        MatchingEngine matchingEngine = new MatchingEngine(balanceManager);
        DiskJournaler diskJournaler = new DiskJournaler("redo_log.bin");

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

        // [ZH] 实例化消费者处理器 / [EN] Instantiate consumer handlers
        JournalEventHandler journalHandler = new JournalEventHandler(diskJournaler);
        MatchingEventHandler matchingHandler = new MatchingEventHandler(matchingEngine);

        // [ZH] 🚀 核心架构升级：绑定消费者 (构建串行依赖：先落盘，后撮合)
        // [EN] 🚀 Core Upgrade: Bind consumers (Build serial dependency: Journal first, Match later)
        disruptor
                .handleEventsWith(journalHandler) // [ZH] 第一步：执行异步记账落盘 / [EN] Step 1: Async journal to disk
                .then(matchingHandler);           // [ZH] 第二步：落盘成功后，触发内存撮合 / [EN] Step 2: Memory match after journaling

        disruptor.start();

        // 3. [ZH] 获取 RingBuffer (生产者发布事件的入口)
        // [EN] Get RingBuffer (Entry point for producers to publish events)
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();

        log.info("Engine is running. Simulating concurrent order placement...");

        // 4. [ZH] 模拟高并发下单 (使用线程池模拟外部 Tomcat 并发)
        // [EN] Simulate high-concurrency order placement (Using thread pool to mock Tomcat concurrency)
        int concurrentThreads = 5; // [ZH] 模拟 5 个外部发单线程 / [EN] Simulate 5 external producer threads
        ExecutorService executor = Executors.newFixedThreadPool(concurrentThreads);

        for (int i = 0; i < 10; i++) { // [ZH] 总共发送 10 笔订单进行测试 / [EN] Send 10 orders in total for testing
            final long orderId = 1000L + i;
            executor.submit(() -> {
                // [ZH] 随机买卖方向 / [EN] Randomize order side for test variety
                OrderSide side = (orderId % 2 == 0) ? OrderSide.BUY : OrderSide.SELL;
                long uid = (side == OrderSide.BUY) ? 100L : 200L;

                publishOrder(ringBuffer, new Order(orderId, uid, side, 50000L, 1000L));
            });
        }

        // [ZH] 关闭线程池并等待任务完成 / [EN] Shutdown executor and wait for completion
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

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
            // [ZH] 🚀 打印当前发单的物理线程名称，证明发单是多线程并发的！
            // [EN] 🚀 Log the current producer thread name to prove concurrent publishing!
            log.info("[Producer] 当前发单线程 / Current Producer Thread: {} | OrderID: {}",
                    Thread.currentThread().getName(), order.getOrderId());

            OrderEvent event = ringBuffer.get(sequence); // [ZH] 获取该坑位的对象 / [EN] Get object at slot
            event.setOrder(order); // [ZH] 填充数据 / [EN] Fill data
        } finally {
            ringBuffer.publish(sequence); // [ZH] 提交发布 (消费者此时才能看到) / [EN] Publish (visible to consumer now)
        }
    }
}