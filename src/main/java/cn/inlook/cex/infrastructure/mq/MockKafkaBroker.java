package cn.inlook.cex.infrastructure.mq;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * [ZH] 模拟高性能指令持久化引擎 (基于 mmap 零拷贝)
 * [EN] Mock High-Performance Command Persistence Engine (Based on mmap Zero-Copy)
 */
@Slf4j
public class MockKafkaBroker {

    // [ZH] 内部指令传输队列 / [EN] Internal command transport queue
    private static final BlockingQueue<Object> COMMAND_TOPIC = new LinkedBlockingQueue<>();

    private static final String JOURNAL_FILE_PATH = "trade_journal_zerocopy.log";

    // [ZH] 预分配 100MB 日志空间 / [EN] Pre-allocate 100MB journal space
    private static final int SEGMENT_SIZE = 100 * 1024 * 1024;

    private static FileChannel fileChannel;
    private static MappedByteBuffer mappedByteBuffer;
    private static int wrotePosition = 0;

    static {
        try {
            File file = new File(JOURNAL_FILE_PATH);
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();

            // [ZH] 建立内存映射文件，实现零拷贝写入
            // [EN] Establish memory-mapped file for zero-copy writing
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, SEGMENT_SIZE);

            log.info("[Journal] Initialized mmap persistence at: {}, Size: {} bytes",
                    file.getAbsolutePath(), SEGMENT_SIZE);

        } catch (IOException e) {
            log.error("[Journal] Failed to initialize persistent storage: ", e);
            throw new RuntimeException("Persistence initialization failed", e);
        }
    }

    /**
     * [ZH] 发送原始指令至持久化层
     * [EN] Send raw command to persistence layer
     * @param command [ZH] 指令载体 (PLACE_ORDER/CANCEL_ORDER) / [EN] Command carrier
     */
    public static void send(Object command) {
        COMMAND_TOPIC.offer(command);
    }

    /**
     * [ZH] 启动持久化消费者线程
     * [EN] Start persistence consumer thread
     */
    public static void startConsumers() {
        Thread writerThread = new Thread(() -> {
            log.info("[Journal] Sequential write thread started.");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // [ZH] 阻塞获取指令数据 / [EN] Blocking take command data
                    Object command = COMMAND_TOPIC.take();

                    // [ZH] 将指令序列化为 JSON 字符串并添加换行符
                    // [EN] Serialize command to JSON string with newline
                    String jsonLine = JSON.toJSONString(command) + System.lineSeparator();
                    byte[] bytes = jsonLine.getBytes(StandardCharsets.UTF_8);

                    if (wrotePosition + bytes.length > SEGMENT_SIZE) {
                        log.error("[Journal] Segment space exhausted. System must halt to prevent data loss.");
                        break;
                    }

                    // [ZH] 执行零拷贝写入 / [EN] Execute zero-copy write
                    mappedByteBuffer.position(wrotePosition);
                    mappedByteBuffer.put(bytes);
                    wrotePosition += bytes.length;

                    // [ZH] 生产环境建议根据吞吐量执行批量 force() / [EN] Recommend batch force() in production
                }
            } catch (InterruptedException e) {
                log.info("[Journal] Writer thread interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                shutdown();
            }
        });

        writerThread.setDaemon(true);
        writerThread.setName("Journal-Writer-Thread");
        writerThread.start();
    }

    /**
     * [ZH] 强制刷盘并释放资源
     * [EN] Force flush and release resources
     */
    public static void shutdown() {
        try {
            if (mappedByteBuffer != null) {
                mappedByteBuffer.force();
            }
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
            }
            log.info("[Journal] Persistent storage resources released.");
        } catch (IOException e) {
            log.error("[Journal] Error during resource release: ", e);
        }
    }
}