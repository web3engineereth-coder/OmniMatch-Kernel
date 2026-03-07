package cn.inlook.cex.infrastructure.disruptor;

import com.lmax.disruptor.EventHandler;
import cn.inlook.cex.infrastructure.journal.DiskJournaler;
import lombok.extern.slf4j.Slf4j;

// [ZH] 记账事件处理器 (第一顺位消费者)
// [EN] Journal Event Handler (First-tier consumer)
@Slf4j
public class JournalEventHandler implements EventHandler<OrderEvent> {

    private final DiskJournaler journaler;

    public JournalEventHandler(DiskJournaler journaler) {
        this.journaler = journaler;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        // [ZH] 🚀 打印当前执行记账逻辑的物理线程名称 (已修复文案)
        // [EN] 🚀 Log the physical thread name executing the journaling logic (Text fixed)
        log.info("[Journaler] 当前记账线程 / Current Journal Thread: {} | Seq: {} | OrderID: {}",
                Thread.currentThread().getName(), sequence, event.getOrder().getOrderId());

        // [ZH] 拦截订单并第一时间落盘。Disruptor 会自动传入分配好的 sequence 序号
        // [EN] Intercept order and persist immediately. Disruptor provides the sequence.
        journaler.append(sequence, event.getOrder());
    }
}