package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * [ZH] 系统恢复管理器：通过重演 AOF (Append Only File) 日志恢复撮合引擎内存状态
 * [EN] Recovery Manager: Replay AOF journals to restore matching engine in-memory state
 */
@Slf4j
public class RecoveryManager {

    private final String journalPath = "trade_journal_zerocopy.log";

    /**
     * [ZH] 执行状态重演：从持久化存储读取指令并重新注入 Disruptor 环形队列
     * [EN] Execute state replay: Read commands from persistence and reinject into Disruptor RingBuffer
     * * @param ringBuffer [ZH] 撮合引擎的输入队列 / [EN] Input RingBuffer of the matching engine
     */
    public void startReplay(RingBuffer<OrderEvent> ringBuffer) {
        if (!Files.exists(Paths.get(journalPath))) {
            log.info("[Recovery] No existing journal file found. Initializing fresh state.");
            return;
        }

        log.info("[Recovery] Starting system state replay from journal: {}", journalPath);

        long startTime = System.currentTimeMillis();
        long commandCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(journalPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                // [ZH] 解析 JSON 指令数据包
                // [EN] Parse JSON command packet
                JSONObject packet = JSON.parseObject(line);
                String typeStr = packet.getString("type");

                if (typeStr == null) continue;

                DisruptorEventType type = DisruptorEventType.valueOf(typeStr);

                // [ZH] 申请 Disruptor 槽位进行状态重演
                // [EN] Claim Disruptor slot for state replay
                long sequence = ringBuffer.next();
                try {
                    OrderEvent event = ringBuffer.get(sequence);
                    event.setEventType(type);

                    if (type == DisruptorEventType.PLACE_ORDER) {
                        // [ZH] 还原订单实体对象
                        // [EN] Restore Order entity object
                        Order order = packet.getObject("data", Order.class);
                        event.setOrder(order);
                    } else if (type == DisruptorEventType.CANCEL_ORDER) {
                        // [ZH] 还原撤单目标 ID
                        // [EN] Restore cancellation target ID
                        event.setCancelOrderId(packet.getLong("data"));
                    }
                } finally {
                    ringBuffer.publish(sequence);
                }
                commandCount++;
            }
        } catch (IOException | IllegalArgumentException e) {
            log.error("[Recovery] Critical error during journal replay: ", e);
            // [ZH] 在金融系统中，恢复失败通常意味着数据不一致，应当中断启动进程
            // [EN] In financial systems, recovery failure implies inconsistency; startup should be halted
            throw new RuntimeException("System recovery failed", e);
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("[Recovery] State replay completed. Restored {} commands in {} ms.", commandCount, duration);
    }
}