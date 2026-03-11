package cn.inlook.cex.infrastructure.mq;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// [ZH] 模拟 Kafka 真实落盘：基于 MappedByteBuffer 的 mmap 零拷贝 (Zero-Copy)
// [EN] Simulate Kafka real persistence: mmap Zero-Copy based on MappedByteBuffer
@Slf4j
public class MockKafkaBroker {

    private static final BlockingQueue<String> TRADE_RESULT_TOPIC = new LinkedBlockingQueue<>();

    private static final String JOURNAL_FILE_PATH = "trade_journal_zerocopy.log";

    // [ZH] 预分配的文件大小 (模拟 Kafka 的 Segment 文件，这里设为 10MB 用于测试)
    // [EN] Pre-allocated file size (Mocking Kafka's Segment file, set to 10MB for testing)
    private static final int SEGMENT_SIZE = 10 * 1024 * 1024;

    private static FileChannel fileChannel;
    private static MappedByteBuffer mappedByteBuffer;

    // [ZH] 记录当前写入的位置
    // [EN] Record the current write position
    private static int wrotePosition = 0;

    static {
        try {
            File file = new File(JOURNAL_FILE_PATH);

            // [ZH] 必须使用 RandomAccessFile 以读写模式 "rw" 打开，才能进行内存映射
            // [EN] Must use RandomAccessFile with "rw" mode to perform memory mapping
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();

            // ==========================================
            // [ZH] 🚀 零拷贝核心：mmap 内存映射
            // [EN] 🚀 Zero-Copy Core: mmap memory mapping
            // ==========================================
            // [ZH] 将文件直接映射到虚拟内存中。对 mappedByteBuffer 的写入，就是直接写入 Page Cache
            // [EN] Map the file directly into virtual memory. Writing to mappedByteBuffer is directly writing to Page Cache
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, SEGMENT_SIZE);

            log.info("[Mock Kafka Zero-Copy] mmap journal initialized at: {}, mapped size: {} bytes",
                    file.getAbsolutePath(), SEGMENT_SIZE);

        } catch (IOException e) {
            log.error("[Mock Kafka Zero-Copy] Failed to initialize mmap.", e);
            System.exit(1);
        }
    }

    public static void send(String message) {
        TRADE_RESULT_TOPIC.offer(message);
    }

    public static void startConsumers() {
        Thread consumerThread = new Thread(() -> {
            log.info("[Mock Kafka Zero-Copy] Disk writer thread started, waiting for trade results...");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String message = TRADE_RESULT_TOPIC.take();

                    String logEntry = message + System.lineSeparator();
                    byte[] bytes = logEntry.getBytes(StandardCharsets.UTF_8);

                    // [ZH] 检查当前 Segment 是否写满 (生产环境中需要切分新的 Segment 文件)
                    // [EN] Check if current Segment is full (Production requires rolling to a new Segment file)
                    if (wrotePosition + bytes.length > SEGMENT_SIZE) {
                        log.warn("[Mock Kafka Zero-Copy] Segment full! In production, roll to a new file here.");
                        // [ZH] 简单模拟：满了就不写了
                        // [EN] Simple mock: Stop writing if full
                        continue;
                    }

                    // ==========================================
                    // [ZH] 极速落盘：没有系统调用 (No System Call)，没有用户态拷贝！
                    // [EN] Ultra-fast persistence: No System Call, No User-Space copy!
                    // ==========================================
                    mappedByteBuffer.position(wrotePosition);
                    mappedByteBuffer.put(bytes);
                    wrotePosition += bytes.length;

                    // [ZH] 异步刷盘：操作系统会在后台自动把 Page Cache 刷入物理磁盘
                    // [EN] Async flush: OS will automatically flush Page Cache to physical disk in the background
                    // [ZH] 注：如果极度追求数据安全性，可调用 mappedByteBuffer.force() 同步刷盘，但这会降低吞吐量
                    // [EN] Note: For extreme data safety, call mappedByteBuffer.force() to sync flush, but it reduces throughput

                    log.info("🚀 [Mock Kafka Zero-Copy] Persisted via mmap: {}", message);
                }
            } catch (InterruptedException e) {
                log.info("[Mock Kafka Zero-Copy] Writer thread interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                try {
                    if (fileChannel != null && fileChannel.isOpen()) {
                        fileChannel.close();
                    }
                } catch (IOException e) {
                    log.error("Error closing file channel", e);
                }
            }
        });

        consumerThread.setDaemon(true);
        consumerThread.setName("Kafka-mmap-Writer-Thread");
        consumerThread.start();
    }
}