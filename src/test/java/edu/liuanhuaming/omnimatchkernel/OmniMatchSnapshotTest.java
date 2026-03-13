package edu.liuanhuaming.omnimatchkernel;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.service.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * [ZH] 二进制快照系统极速 I/O 基准测试
 * [EN] Binary Snapshot System Extreme I/O Benchmark Test
 */
public class OmniMatchSnapshotTest {

    private static final Logger log = LoggerFactory.getLogger(OmniMatchSnapshotTest.class);
    private static final int TOTAL_ORDERS = 1_000_000;
    private static final String SNAPSHOT_FILE = "engine_snapshot.bin";

    @Test
    public void testBinarySnapshotThroughput() {
        log.warn("=====================================================");
        log.warn("[SNAPSHOT BENCHMARK] Generating {} mock orders in memory...", TOTAL_ORDERS);

        // 1. [ZH] 构造 100 万个活跃订单数据
        // [EN] Construct 1,000,000 active order data
        List<Order> mockOrders = new ArrayList<>(TOTAL_ORDERS);
        for (long i = 1; i <= TOTAL_ORDERS; i++) {
            OrderSide side = (i % 2 == 0) ? OrderSide.BUY : OrderSide.SELL;
            long price = 60000 + (i % 100);
            Order order = new Order(i, 1001L, side, price, 10L);
            mockOrders.add(order);
        }

        SnapshotManager snapshotManager = new SnapshotManager();

        // ==========================================
        // 2. [ZH] 测试二进制 Dump (写入) 性能
        // [EN] Test Binary Dump (Write) Performance
        // ==========================================
        log.warn("[SNAPSHOT BENCHMARK] Executing binary memory dump...");
        long dumpStartTime = System.currentTimeMillis();

        snapshotManager.saveSnapshot(mockOrders);

        long dumpTimeMs = System.currentTimeMillis() - dumpStartTime;

        // [ZH] 计算文件大小与压缩率
        // [EN] Calculate file size and compression ratio
        File file = new File(SNAPSHOT_FILE);
        long fileSizeBytes = file.length();
        double fileSizeMB = fileSizeBytes / (1024.0 * 1024.0);

        log.warn("Dump Completed in: {} ms", dumpTimeMs);
        log.warn("Snapshot File Size: {} MB (Avg {} bytes/order)",
                String.format("%.2f", fileSizeMB),
                fileSizeBytes / TOTAL_ORDERS);
        if (dumpTimeMs > 0) {
            log.warn("Dump Throughput: {} ops/sec", String.format("%.2f", (TOTAL_ORDERS * 1000.0) / dumpTimeMs));
        }

        // ==========================================
        // 3. [ZH] 测试二进制 Load (读取) 性能
        // [EN] Test Binary Load (Read) Performance
        // ==========================================
        log.warn("-----------------------------------------------------");
        log.warn("[SNAPSHOT BENCHMARK] Executing binary memory load...");
        long loadStartTime = System.currentTimeMillis();

        Map<Long, Order> restoredOrders = snapshotManager.loadSnapshot();

        long loadTimeMs = System.currentTimeMillis() - loadStartTime;

        log.warn("Load Completed in: {} ms", loadTimeMs);
        if (loadTimeMs > 0) {
            log.warn("Load Throughput: {} ops/sec", String.format("%.2f", (TOTAL_ORDERS * 1000.0) / loadTimeMs));
        }
        log.warn("=====================================================");

        // 4. [ZH] 数据一致性严苛校验
        // [EN] Strict data consistency validation
        assertEquals(TOTAL_ORDERS, restoredOrders.size(), "Restored order count mismatch!");

        Order sampleOrder = restoredOrders.get(999_999L);
        assertNotNull(sampleOrder, "Sample order not found!");
        assertEquals(999_999L, sampleOrder.getOrderId());
        assertEquals(10L, sampleOrder.getOriginalAmount());

        // [ZH] 测试完成后可选择清理物理文件 (当前保留以便观察)
        // [EN] Optionally clean up physical file after test (kept here for observation)
        // file.delete();
    }
}