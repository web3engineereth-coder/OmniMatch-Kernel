package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.service.MatchingEngine;
import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;

// [ZH] 撮合事件处理器 (消费者) - 保证单线程执行，绝对无锁！
// [EN] Matching Event Handler (Consumer) - Guarantees single-threaded execution, lock-free!
@Slf4j
public class MatchingEventHandler implements EventHandler<OrderEvent> {

    private final MatchingEngine engine;

    public MatchingEventHandler(MatchingEngine engine) {
        this.engine = engine;
    }

    // [ZH] 每次 RingBuffer 有新订单，都会触发这个方法
    // [EN] Triggered every time there is a new order in the RingBuffer
    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            // [ZH] 极速调用核心撮合逻辑
            // [EN] Execute core matching logic at extreme speed
            engine.processOrder(event.getOrder());

            // [ZH] TODO: endOfBatch 为 true 时，可以触发批量持久化或 WebSocket 推送
            // [EN] TODO: When endOfBatch is true, trigger batch persistence or WebSocket push
        } catch (Exception e) {
            // [ZH] 必须捕获异常，否则会导致整个消费线程崩溃
            // [EN] Must catch exceptions to prevent the consumer thread from dying
            log.error("Fatal error during matching order: {}", event.getOrder().getOrderId(), e);
        } finally {
            // [ZH] 帮助 GC (清理对象引用)
            // [EN] Help GC (Clear object reference)
            event.setOrder(null);
        }
    }
}