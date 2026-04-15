package cn.inlook.cex.infrastructure.journal;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import lombok.Getter;

// [ZH] 可回放命令日志条目，统一 PLACE / CANCEL / SNAPSHOT 三类输入指令
// [EN] Replayable command journal entry unifying PLACE / CANCEL / SNAPSHOT input commands
@Getter
public class CommandJournalEntry {

    private final long sequence;
    private final DisruptorEventType type;
    private final PlaceOrderCommand placeOrderCommand;
    private final CancelOrderCommand cancelOrderCommand;

    private CommandJournalEntry(long sequence,
                                DisruptorEventType type,
                                PlaceOrderCommand placeOrderCommand,
                                CancelOrderCommand cancelOrderCommand) {
        this.sequence = sequence;
        this.type = type;
        this.placeOrderCommand = placeOrderCommand;
        this.cancelOrderCommand = cancelOrderCommand;
    }

    public static CommandJournalEntry forPlaceOrder(long sequence, PlaceOrderCommand command) {
        return new CommandJournalEntry(sequence, DisruptorEventType.PLACE_ORDER, command, null);
    }

    public static CommandJournalEntry forCancelOrder(long sequence, CancelOrderCommand command) {
        return new CommandJournalEntry(sequence, DisruptorEventType.CANCEL_ORDER, null, command);
    }

    public static CommandJournalEntry forSnapshot(long sequence) {
        return new CommandJournalEntry(sequence, DisruptorEventType.MAKE_SNAPSHOT, null, null);
    }
}
