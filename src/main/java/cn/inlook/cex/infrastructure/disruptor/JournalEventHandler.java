package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import com.lmax.disruptor.EventHandler;
import cn.inlook.cex.infrastructure.journal.DiskJournaler;
import cn.inlook.cex.infrastructure.journal.CommandJournalEntry;
import cn.inlook.cex.infrastructure.journal.CommandJournalWriter;
import lombok.extern.slf4j.Slf4j;

// [ZH] 记账事件处理器 (第一顺位消费者)
// [EN] Journal Event Handler (First-tier consumer)
@Slf4j
public class JournalEventHandler implements EventHandler<OrderEvent> {

    private final DiskJournaler journaler;
    private final CommandJournalWriter commandJournalWriter;

    public JournalEventHandler(DiskJournaler journaler) {
        this(journaler, new CommandJournalWriter());
    }

    public JournalEventHandler(DiskJournaler journaler, CommandJournalWriter commandJournalWriter) {
        this.journaler = journaler;
        this.commandJournalWriter = commandJournalWriter;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        if (event.getEventType() == DisruptorEventType.MAKE_SNAPSHOT) {
            commandJournalWriter.append(CommandJournalEntry.forSnapshot(sequence));
            return;
        }

        if (event.getEventType() == DisruptorEventType.CANCEL_ORDER) {
            CancelOrderCommand command = event.getCancelOrderCommand();
            if (command == null && event.getCancelOrderId() != 0L) {
                command = new CancelOrderCommand(event.getCancelOrderId(), 0L, System.nanoTime());
            }
            if (command != null) {
                commandJournalWriter.append(CommandJournalEntry.forCancelOrder(sequence, command));
            }
            return;
        }

        if (event.getEventType() != DisruptorEventType.PLACE_ORDER) {
            return;
        }

        Order order = event.getOrder();
        PlaceOrderCommand placeOrderCommand = event.getPlaceOrderCommand();
        if (order == null) {
            if (placeOrderCommand == null) {
                return;
            }
            order = new Order(
                    placeOrderCommand.getOrderId(),
                    placeOrderCommand.getUserId(),
                    placeOrderCommand.getSymbol(),
                    placeOrderCommand.getSide(),
                    placeOrderCommand.getPrice(),
                    placeOrderCommand.getQuantity());
            order.setTimestamp(placeOrderCommand.getTimestamp());
        } else if (placeOrderCommand == null) {
            placeOrderCommand = new PlaceOrderCommand(
                    order.getOrderId(),
                    order.getUserId(),
                    order.getSymbol(),
                    order.getSide(),
                    order.getPrice(),
                    order.getRemainingAmount(),
                    order.getTimestamp());
        }

        // [ZH] 🚀 打印当前执行记账逻辑的物理线程名称 (已修复文案)
        // [EN] 🚀 Log the physical thread name executing the journaling logic (Text fixed)
        log.info("[Journaler] 当前记账线程 / Current Journal Thread: {} | Seq: {} | OrderID: {}",
                Thread.currentThread().getName(), sequence, order.getOrderId());

        // [ZH] 拦截订单并第一时间落盘。Disruptor 会自动传入分配好的 sequence 序号
        // [EN] Intercept order and persist immediately. Disruptor provides the sequence.
        journaler.append(sequence, order);
        commandJournalWriter.append(CommandJournalEntry.forPlaceOrder(sequence, placeOrderCommand));
    }
}
