package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.infrastructure.disruptor.DisruptorEventType;
import cn.inlook.cex.infrastructure.disruptor.OrderEvent;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONException;
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
     * @param ringBuffer [ZH] 撮合引擎的输入队列 / [EN] Input RingBuffer of the matching engine
     */
    public void startReplay(RingBuffer<OrderEvent> ringBuffer) {
        if (!Files.exists(Paths.get(journalPath))) {
            log.info("[Recovery] No existing journal file found. Initializing fresh state.");
            return;
        }

        log.info("[Recovery] Starting system state replay from journal: {}", journalPath);

        long startTime = System.currentTimeMillis();
        long commandCount = 0;
        long skipCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(journalPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // [ZH] 过滤空行或明显不符合 JSON 格式的脏数据 (mmap 预分配可能产生的空位)
                // [EN] Filter empty lines or obvious junk data (potential nulls from mmap pre-allocation)
                if (line.isEmpty() || !line.startsWith("{")) {
                    continue;
                }

                try {
                    // [ZH] 解析 JSON 指令数据包
                    // [EN] Parse JSON command packet
                    JSONObject packet = JSON.parseObject(line);

                    // [ZH] 核心改动：必须包含 type 字段才是有效的恢复指令
                    // [EN] Core change: Must contain 'type' field to be a valid recovery command
                    if (!packet.containsKey("type")) {
                        skipCount++;
                        continue;
                    }

                    String typeStr = packet.getString("type");
                    DisruptorEventType type = DisruptorEventType.valueOf(typeStr);

                    // [ZH] 申请 Disruptor 槽位进行状态重演
                    // [EN] Claim Disruptor slot for state replay
                    long sequence = ringBuffer.next();
                    try {
                        OrderEvent event = ringBuffer.get(sequence);
                        event.setEventType(type);

                        if (type == DisruptorEventType.PLACE_ORDER) {
                            Order order = packet.getObject("data", Order.class);
                            event.setOrder(order);
                        } else if (type == DisruptorEventType.CANCEL_ORDER) {
                            event.setCancelOrderId(packet.getLong("data"));
                        }
                    } finally {
                        ringBuffer.publish(sequence);
                    }
                    commandCount++;

                } catch (JSONException | IllegalArgumentException e) {
                    // [ZH] 记录行解析错误但允许继续执行，提高系统的容错生存能力
                    // [EN] Log line parsing error but allow continuation to improve system resilience
                    log.warn("[Recovery] Skipping corrupted or invalid line: {}", line);
                    skipCount++;
                }
            }
        } catch (IOException e) {
            log.error("[Recovery] Critical I/O error during journal replay: ", e);
            throw new RuntimeException("System recovery aborted due to I/O failure", e);
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("[Recovery] State replay completed. Restored: {}, Skipped: {}, Time: {} ms",
                commandCount, skipCount, duration);
    }
}