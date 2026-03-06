package cn.inlook.cex.infrastructure.journal;

import cn.inlook.cex.domain.model.Order;
import lombok.extern.slf4j.Slf4j;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

@Slf4j
public class DiskJournaler {
    private MappedByteBuffer buffer;

    // [ZH] 预分配 100MB 空间，避免动态扩容带来的 I/O 抖动
    // [EN] Pre-allocate 100MB to avoid I/O jitter from dynamic expansion
    private static final int FILE_SIZE = 1024 * 1024 * 100;

    public DiskJournaler(String filePath) {
        try {
            // [ZH] 利用 Mmap 技术将磁盘文件映射到内存地址空间 (零拷贝)
            // [EN] Map disk file to memory address space using Mmap (Zero-copy)
            RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
            this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
            log.info("DiskJournaler initialized, mapped file: {}", filePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize journal", e);
        }
    }

    public void append(long sequence, Order order) {
        // [ZH] 顺序追加写入 Page Cache，速度达到微秒级
        // [EN] Sequential append to Page Cache, achieving microsecond latency
        buffer.putLong(sequence);
        buffer.putLong(order.getOrderId());
        buffer.putLong(order.getPrice());
        buffer.putLong(order.getRemainingAmount());

        // [ZH] 将 OrderSide 枚举转换为 byte 存储 (0为BUY, 1为SELL)
        // [EN] Convert OrderSide enum to byte for storage (0 for BUY, 1 for SELL)
        buffer.put((byte) (order.getSide().name().equals("BUY") ? 0 : 1));
    }
}