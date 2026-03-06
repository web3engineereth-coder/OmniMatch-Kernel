package cn.inlook.cex.infrastructure.mq;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// [ZH] 纯内存模拟 Kafka，用于学习异步解耦思想
// [EN] In-memory Mock Kafka to learn async decoupling concepts
@Slf4j
public class MockKafkaBroker {

    // [ZH] 模拟 Kafka 的 Topic: cex.trade.results (无界阻塞队列)
    // [EN] Mock Kafka Topic: cex.trade.results (Unbounded blocking queue)
    private static final BlockingQueue<String> TRADE_RESULT_TOPIC = new LinkedBlockingQueue<>();

    // [ZH] 生产者发送消息 (模拟 Kafka Producer)
    // [EN] Producer sends message (Mocking Kafka Producer)
    public static void send(String message) {
        TRADE_RESULT_TOPIC.offer(message);
    }

    // [ZH] 启动下游消费者 (模拟 Elasticsearch 索引器 / K线推送服务)
    // [EN] Start downstream consumers (Mocking ES Indexer / K-Line Push)
    public static void startConsumers() {
        Thread consumerThread = new Thread(() -> {
            log.info("[Mock Kafka] Consumer group started, waiting for trade results...");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // [ZH] 阻塞获取消息，拿到后进行耗时的下游操作
                    // [EN] Blocking take, then perform time-consuming downstream operations
                    String message = TRADE_RESULT_TOPIC.take();

                    // [ZH] 这里模拟网络 I/O 或数据库写入延迟 (不阻塞主撮合线程！)
                    // [EN] Simulate Network I/O or DB write latency (Does NOT block matching thread!)
                    Thread.sleep(50);
                    log.info("📥 [Mock Kafka Downstream] Successfully indexed to ES / Updated K-Line: {}", message);
                }
            } catch (InterruptedException e) {
                log.info("[Mock Kafka] Consumer interrupted and shutting down.");
                Thread.currentThread().interrupt();
            }
        });
        consumerThread.setDaemon(true); // [ZH] 设置为守护线程 / [EN] Set as daemon thread
        consumerThread.setName("Kafka-Consumer-Thread");
        consumerThread.start();
    }
}