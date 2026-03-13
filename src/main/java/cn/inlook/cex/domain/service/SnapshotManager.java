package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * [ZH] 二进制快照管理器：实现内存状态的极速 Dump 与 Load
 * [EN] Binary Snapshot Manager: Implements ultra-fast Dump and Load of memory state
 */
public class SnapshotManager {

    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);
    private static final String SNAPSHOT_FILE = "engine_snapshot.bin";

    // [ZH] 魔数，用于校验文件类型是否正确 (类似 Java class 的 CAFEBABE)
    // [EN] Magic number for file type validation
    private static final int MAGIC_NUMBER = 0x0308CE;

    /**
     * [ZH] 创建系统快照 (Dump)
     * [EN] Create system snapshot (Dump)
     * @param activeOrders [ZH] 当前引擎中所有存活的订单 / [EN] All active orders in the engine
     */
    public void saveSnapshot(Collection<Order> activeOrders) {
        log.warn("[Snapshot] Starting binary memory dump. Target orders: {}", activeOrders.size());
        long startTime = System.currentTimeMillis();

        // [ZH] 使用 DataOutputStream 配合 BufferedOutputStream 追求极致写入性能
        // [EN] Use DataOutputStream with BufferedOutputStream for extreme write performance
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(SNAPSHOT_FILE)))) {

            // 1. [ZH] 写入文件头：魔数 + 订单总数
            // [EN] Write file header: Magic Number + Total Orders
            dos.writeInt(MAGIC_NUMBER);
            dos.writeInt(activeOrders.size());

            // 2. [ZH] 密集写入订单二进制数据
            // [EN] Densely write order binary data
            for (Order order : activeOrders) {
                dos.writeLong(order.getOrderId());
                dos.writeLong(order.getUserId());
                dos.writeByte(order.getSide() == OrderSide.BUY ? 1 : 0);
                dos.writeLong(order.getPrice());
                dos.writeLong(order.getOriginalAmount());
                dos.writeLong(order.getRemainingAmount());
                dos.writeLong(order.getTimestamp());
                dos.writeBoolean(order.isCanceled());
            }

            dos.flush();
            long duration = System.currentTimeMillis() - startTime;
            log.warn("[Snapshot] Binary dump completed! Saved {} orders in {} ms.", activeOrders.size(), duration);

        } catch (IOException e) {
            log.error("[Snapshot] Fatal error during memory dump: ", e);
        }
    }

    /**
     * [ZH] 加载系统快照 (Load)
     * [EN] Load system snapshot (Load)
     * @return [ZH] 恢复后的订单映射表 / [EN] Restored order map
     */
    public Map<Long, Order> loadSnapshot() {
        File file = new File(SNAPSHOT_FILE);
        if (!file.exists()) {
            log.info("[Snapshot] No existing snapshot found. Starting from scratch.");
            return new HashMap<>();
        }

        log.warn("[Snapshot] Loading binary snapshot from disk...");
        long startTime = System.currentTimeMillis();
        Map<Long, Order> restoredOrders = new HashMap<>();

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {

            // 1. [ZH] 校验文件头
            // [EN] Validate file header
            int magic = dis.readInt();
            if (magic != MAGIC_NUMBER) {
                throw new IllegalStateException("Invalid snapshot file format! Magic number mismatch.");
            }

            int orderCount = dis.readInt();

            // 2. [ZH] 密集读取并重建对象
            // [EN] Densely read and reconstruct objects
            for (int i = 0; i < orderCount; i++) {
                Order order = new Order();
                order.setOrderId(dis.readLong());
                order.setUserId(dis.readLong());
                order.setSide(dis.readByte() == 1 ? OrderSide.BUY : OrderSide.SELL);
                order.setPrice(dis.readLong());
                order.setOriginalAmount(dis.readLong());
                order.setRemainingAmount(dis.readLong());
                order.setTimestamp(dis.readLong());

                boolean isCanceled = dis.readBoolean();
                if (isCanceled) {
                    order.cancel();
                }

                restoredOrders.put(order.getOrderId(), order);
            }

            long duration = System.currentTimeMillis() - startTime;
            log.warn("[Snapshot] Binary snapshot loaded! Restored {} orders in {} ms.", orderCount, duration);
            return restoredOrders;

        } catch (IOException e) {
            log.error("[Snapshot] Fatal error during snapshot loading: ", e);
            throw new RuntimeException("System recovery aborted due to snapshot corruption.", e);
        }
    }
}