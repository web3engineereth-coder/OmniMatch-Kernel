package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.service.MatchingEngine;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import lombok.extern.slf4j.Slf4j;

/**
 * [ZH] 撮合事件处理器 (消费者) - 包含进度追踪与指令路由
 * [EN] Matching Event Handler (Consumer) - Includes sequence tracking and command routing
 */
@Slf4j
public class MatchingEventHandler implements EventHandler<OrderEvent> {

    private final MatchingEngine engine;

    // ==========================================
    // [ZH] 🚀 核心：手动维护消费进度序列，初始值为 -1
    // [ZH] 这样压测类通过 handler.getSequence().get() 就能知道消费者的实时进度
    // [EN] 🚀 Core: Manually maintain the consumer sequence, initialized to -1
    // [EN] This allows the benchmark class to track progress via handler.getSequence().get()
    // ==========================================
    private final Sequence sequence = new Sequence(-1);

    public MatchingEventHandler(MatchingEngine engine) {
        this.engine = engine;
    }

    /**
     * [ZH] 获取当前消费者的进度序列
     * [EN] Get the current consumer's sequence
     */
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            // [ZH] 提取目标 ID 用于日志 (仅在 DEBUG/INFO 级别有效)
            // [EN] Extract target ID for logging
            long targetOrderId = (event.getEventType() == DisruptorEventType.PLACE_ORDER && event.getOrder() != null)
                    ? event.getOrder().getOrderId()
                    : event.getCancelOrderId();

            // [ZH] 注意：压测时请务必关闭此日志，否则 TPS 会被 I/O 卡死
            // [EN] Note: MUST disable this log during benchmark to avoid I/O bottleneck
            if (log.isDebugEnabled()) {
                log.debug("[Matcher] Seq: {} | Type: {} | TargetID: {}",
                        sequence, event.getEventType(), targetOrderId);
            }

            // ==========================================
            // [ZH] 指令路由分发
            // [EN] Command routing dispatch
            // ==========================================
            if (event.getEventType() == DisruptorEventType.PLACE_ORDER) {
                Order incomingOrder = event.getOrder();
                if (incomingOrder != null) {
                    engine.processOrder(incomingOrder);
                }
            } else if (event.getEventType() == DisruptorEventType.CANCEL_ORDER) {
                // [ZH] 执行 O(1) 极限撤单
                engine.cancelOrder(event.getCancelOrderId());
            }

        } catch (Exception e) {
            log.error("Fatal error during matching process at seq {}: ", sequence, e);
        } finally {
            // ==========================================
            // [ZH] 🚀 极其重要：
            // [ZH] 1. 更新进度，让生产者（压测类）知道我们已经处理完这一条了
            // [ZH] 2. 清理引用，防止 RingBuffer 导致内存泄漏
            // ==========================================
            this.sequence.set(sequence);
            event.clear();
        }
    }
}