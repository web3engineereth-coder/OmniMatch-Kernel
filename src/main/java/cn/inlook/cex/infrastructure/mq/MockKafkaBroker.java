package cn.inlook.cex.infrastructure.mq;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * [ZH] 模拟高性能指令持久化引擎 (基于 mmap 零拷贝)
 * [EN] Mock High-Performance Command Persistence Engine (Based on mmap Zero-Copy)
 */
@Slf4j
public class MockKafkaBroker {

    private static final BlockingQueue<Object> COMMAND_TOPIC = new LinkedBlockingQueue<>();
    private static final String JOURNAL_FILE_PATH = "trade_journal_zerocopy.log";
    private static final int SEGMENT_SIZE = 100 * 1024 * 1024;

    private static FileChannel fileChannel;
    private static MappedByteBuffer mappedByteBuffer;
    private static volatile int wrotePosition = 0;
    private static volatile boolean isRunning = true;
    private static Thread writerThread;

    static {
        try {
            File file = new File(JOURNAL_FILE_PATH);
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            fileChannel = raf.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, SEGMENT_SIZE);
            log.info("[Journal] Initialized mmap persistence at: {}", file.getAbsolutePath());
        } catch (IOException e) {
            log.error("[Journal] Failed to initialize persistent storage: ", e);
            throw new RuntimeException("Persistence initialization failed", e);
        }
    }

    public static void send(Object command) {
        COMMAND_TOPIC.offer(command);
    }

    public static void startConsumers() {
        isRunning = true;
        writerThread = new Thread(() -> {
            log.info("[Journal] Sequential write thread started.");
            try {
                while (isRunning || !COMMAND_TOPIC.isEmpty()) {
                    Object command = COMMAND_TOPIC.poll(10, TimeUnit.MILLISECONDS);
                    if (command == null) continue;

                    String jsonLine = JSON.toJSONString(command) + System.lineSeparator();
                    byte[] bytes = jsonLine.getBytes(StandardCharsets.UTF_8);

                    if (wrotePosition + bytes.length > SEGMENT_SIZE) {
                        log.error("[Journal] Segment space exhausted!");
                        break;
                    }

                    mappedByteBuffer.put(wrotePosition, bytes);
                    wrotePosition += bytes.length;
                }
            } catch (InterruptedException e) {
                log.info("[Journal] Writer thread interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                doReleaseResources();
            }
        });

        writerThread.setDaemon(false);
        writerThread.setName("Journal-Writer-Thread");
        writerThread.start();
    }

    /**
     * [ZH] 优雅停机：同步等待写入完成并释放资源
     * [EN] Graceful Shutdown: Synchronously wait for completion and release resources
     */
    public static void shutdown() {
        isRunning = false;
        try {
            if (writerThread != null && writerThread.isAlive()) {
                writerThread.join();
            }
        } catch (InterruptedException e) {
            log.error("[Journal] Shutdown join interrupted: ", e);
            Thread.currentThread().interrupt();
        }
    }

    private static synchronized void doReleaseResources() {
        try {
            if (mappedByteBuffer != null) {
                mappedByteBuffer.force();
                // [ZH] 强制解除内存映射，否则文件会被系统锁定，导致 truncate 失效
                // [EN] Force unmap to release file lock, otherwise truncate will fail
                unmap(mappedByteBuffer);
                mappedByteBuffer = null;
            }
            if (fileChannel != null && fileChannel.isOpen()) {
                // [ZH] 物理截断文件至实际大小，彻底消除末尾 NUL 字符
                // [EN] Truncate file to actual size to eliminate trailing NUL characters
                fileChannel.truncate(wrotePosition);
                fileChannel.close();
                log.info("[Journal] Storage released. Final size: {} bytes", wrotePosition);
            }
        } catch (IOException e) {
            log.error("[Journal] Error during resource release: ", e);
        }
    }

    /**
     * [ZH] 强行回收 MappedByteBuffer
     * [EN] Forcibly unmap MappedByteBuffer
     */
    private static void unmap(MappedByteBuffer buffer) {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            unsafe.invokeCleaner(buffer);
        } catch (Exception e) {
            log.warn("[Journal] Failed to unmap buffer: {}", e.getMessage());
        }
    }
}