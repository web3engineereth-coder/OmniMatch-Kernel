package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * [ZH] 系统恢复管理器：二进制快照秒级装载 + AOF 增量日志断点重演
 * [EN] Recovery Manager: Sub-second binary snapshot load + AOF incremental journal replay
 */
@Slf4j
public class RecoveryManager {

    private final String journalPath = "trade_journal_zerocopy.log";

    // [ZH] 🚀 核心改动 1：不再传入 RingBuffer，而是直接持有引擎的引用
    // [EN] 🚀 Core Change 1: Hold Engine reference directly instead of RingBuffer
    private final MatchingEngine engine;
    private final SnapshotManager snapshotManager;

    public RecoveryManager(MatchingEngine engine) {
        this.engine = engine;
        this.snapshotManager = new SnapshotManager();
    }

    /**
     * [ZH] 执行极速恢复：加载快照 -> 寻找断点 -> 裸跑引擎重演
     * [EN] Execute ultra-fast recovery: Load snapshot -> Seek breakpoint -> Bare-metal engine replay
     */
    public void startReplay() {
        log.info("=====================================================");
        log.info("[Recovery] Initializing OmniMatch Kernel State...");
        long globalStartTime = System.currentTimeMillis();

        // ==========================================
        // 阶段 1：[ZH] 极速加载二进制快照 (Base State)
        // ==========================================
        Map<Long, Order> restoredOrders = snapshotManager.loadSnapshot();
        boolean hasSnapshot = !restoredOrders.isEmpty();

        if (hasSnapshot) {
            // [ZH] 🚀 直接调用引擎刚刚开放的后门方法，瞬间注满内存
            engine.restoreFromSnapshot(restoredOrders);
            log.info("[Recovery] Stage 1: Snapshot baseline injected into engine. ({} orders)", restoredOrders.size());
        }

        // ==========================================
        // 阶段 2：[ZH] 增量重演 AOF 日志 (Incremental Replay)
        // ==========================================
        if (!Files.exists(Paths.get(journalPath))) {
            log.info("[Recovery] No existing journal file found. Starting as fresh instance.");
            return;
        }

        log.info("[Recovery] Stage 2: Scanning journal for incremental commands: {}", journalPath);

        long commandCount = 0;
        long skipCount = 0;
        long historySkippedCount = 0; // [ZH] 记录因为快照而跳过的历史日志行数

        // [ZH] 断点标志：如果存在快照，我们需要在日志中找到那根 "红线"
        boolean snapshotMarkerFound = !hasSnapshot;

        try (BufferedReader reader = new BufferedReader(new FileReader(journalPath), 1024 * 1024)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // [ZH] 保留你原本优秀的容错逻辑：过滤空洞与脏数据
                if (line.isEmpty() || !line.startsWith("{")) {
                    continue;
                }

                // [ZH] 🚀 核心改动 2：断点寻址机制。在找到 MAKE_SNAPSHOT 之前，无脑跳过
                if (!snapshotMarkerFound) {
                    if (line.contains("\"MAKE_SNAPSHOT\"")) {
                        snapshotMarkerFound = true;
                        log.info("[Recovery] >>> Found MAKE_SNAPSHOT marker! Skipped {} historical lines. Commencing incremental replay...", historySkippedCount);
                    } else {
                        historySkippedCount++;
                    }
                    continue;
                }

                try {
                    JSONObject packet = JSON.parseObject(line);

                    if (!packet.containsKey("type")) {
                        skipCount++;
                        continue;
                    }

                    String typeStr = packet.getString("type");
                    DisruptorEventType type = DisruptorEventType.valueOf(typeStr);

                    // [ZH] 🚀 核心改动 3：绕过 RingBuffer，以单线程最高速度直接向引擎下达指令
                    if (type == DisruptorEventType.PLACE_ORDER) {
                        Order order = packet.getObject("data", Order.class);
                        engine.processOrder(order); // 直连！
                        commandCount++;
                    } else if (type == DisruptorEventType.CANCEL_ORDER) {
                        long cancelOrderId = packet.getLongValue("data");
                        engine.cancelOrder(cancelOrderId); // 直连！
                        commandCount++;
                    }

                } catch (JSONException | IllegalArgumentException e) {
                    // [ZH] 依然保留你的单行容错能力
                    log.warn("[Recovery] Skipping corrupted or invalid line: {}", line);
                    skipCount++;
                }
            }
        } catch (IOException e) {
            log.error("[Recovery] Critical I/O error during journal replay: ", e);
            throw new RuntimeException("System recovery aborted due to I/O failure", e);
        }

        long duration = System.currentTimeMillis() - globalStartTime;
        log.info("=====================================================");
        log.info("[Recovery] 🚀 KERNEL RECOVERY COMPLETED IN {} ms!", duration);
        log.info(" - Snapshot Orders: {}", restoredOrders.size());
        log.info(" - History Skipped: {}", historySkippedCount);
        log.info(" - Replayed Events: {}", commandCount);
        log.info(" - Corrupted Skipped: {}", skipCount);
        if (duration > 0 && commandCount > 0) {
            log.info(" - Replay Speed:    {} ops/sec", String.format("%.2f", (commandCount * 1000.0) / duration));
        }
        log.info("=====================================================");
    }
}