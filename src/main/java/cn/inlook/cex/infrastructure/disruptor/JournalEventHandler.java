package cn.inlook.cex.infrastructure.disruptor;

import com.lmax.disruptor.EventHandler;
import cn.inlook.cex.infrastructure.journal.DiskJournaler;

public class JournalEventHandler implements EventHandler<OrderEvent> {

    private final DiskJournaler journaler;

    public JournalEventHandler(DiskJournaler journaler) {
        this.journaler = journaler;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        // [ZH] 拦截订单并第一时间落盘。Disruptor 会自动传入分配好的 sequence 序号
        // [EN] Intercept order and persist immediately. Disruptor provides the sequence.
        journaler.append(sequence, event.getOrder());
    }
}