package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.service.InMemoryAccountService;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.RecoveryManager;
import cn.inlook.cex.domain.service.SnapshotManager;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.JournalEventHandler;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import cn.inlook.cex.infrastructure.journal.CommandJournalWriter;
import cn.inlook.cex.infrastructure.journal.DiskJournaler;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class CommandJournalReplayTest {

    @Test
    void shouldReplayCommandJournalAndRestorePartialFillCancelLifecycle() throws Exception {
        Path tempDir = Files.createTempDirectory("command-journal-replay");
        Path redoLogPath = tempDir.resolve("redo_log.bin");
        Path commandJournalPath = tempDir.resolve("trade_journal_zerocopy.log");
        Path snapshotPath = tempDir.resolve("engine_snapshot.bin");

        CommandJournalWriter commandJournalWriter = new CommandJournalWriter(commandJournalPath.toString());
        JournalEventHandler journalEventHandler = new JournalEventHandler(
                new DiskJournaler(redoLogPath.toString()),
                commandJournalWriter
        );

        OrderEvent placeSellEvent = new OrderEvent();
        placeSellEvent.setEventType(DisruptorEventType.PLACE_ORDER);
        placeSellEvent.setPlaceOrderCommand(new PlaceOrderCommand(1L, 1001L, "DEFAULT", OrderSide.SELL, 100L, 10L, 11L));
        journalEventHandler.onEvent(placeSellEvent, 1L, true);

        OrderEvent placeBuyEvent = new OrderEvent();
        placeBuyEvent.setEventType(DisruptorEventType.PLACE_ORDER);
        placeBuyEvent.setPlaceOrderCommand(new PlaceOrderCommand(2L, 1002L, "DEFAULT", OrderSide.BUY, 100L, 5L, 12L));
        journalEventHandler.onEvent(placeBuyEvent, 2L, true);

        OrderEvent cancelEvent = new OrderEvent();
        cancelEvent.setEventType(DisruptorEventType.CANCEL_ORDER);
        cancelEvent.setCancelOrderCommand(new CancelOrderCommand("DEFAULT", 1L, 1001L, 13L));
        journalEventHandler.onEvent(cancelEvent, 3L, true);
        commandJournalWriter.close();

        InMemoryAccountService recoveredAccountService = new InMemoryAccountService();
        recoveredAccountService.createAccount(1001L, 0L, 10L);
        recoveredAccountService.createAccount(1002L, 1_000L, 0L);
        MatchingEngine recoveredEngine = new MatchingEngine(recoveredAccountService);
        RecoveryManager recoveryManager = new RecoveryManager(
                recoveredEngine,
                new SnapshotManager(snapshotPath.toString()),
                commandJournalPath.toString()
        );

        recoveryManager.startReplay();

        Account seller = recoveredAccountService.getAccount(1001L);
        Account buyer = recoveredAccountService.getAccount(1002L);

        assertEquals(500L, seller.getAvailableCash());
        assertEquals(5L, seller.getAvailableAsset());
        assertEquals(0L, seller.getFrozenAsset());

        assertEquals(500L, buyer.getAvailableCash());
        assertEquals(5L, buyer.getAvailableAsset());
        assertEquals(0L, buyer.getFrozenCash());

        assertFalse(recoveredEngine.hasActiveOrder(1L));
        assertFalse(recoveredEngine.hasActiveOrder(2L));
        assertEquals(0, recoveredEngine.getActiveOrderCount());
        assertNull(recoveredEngine.getBestAskPrice());
        assertNull(recoveredEngine.getBestBidPrice());
        recoveredEngine.assertInvariant();
    }
}
