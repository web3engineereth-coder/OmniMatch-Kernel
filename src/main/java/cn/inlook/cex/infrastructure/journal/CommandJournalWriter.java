package cn.inlook.cex.infrastructure.journal;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

// [ZH] 最小命令日志写入器：将标准化命令顺序落盘，供恢复期直接回放
// [EN] Minimal command journal writer that persists standardized commands for direct recovery replay
@Slf4j
public class CommandJournalWriter implements Closeable {

    public static final String DEFAULT_JOURNAL_PATH = "trade_journal_zerocopy.log";

    private final Path journalPath;
    private final BufferedWriter writer;

    public CommandJournalWriter() {
        this(DEFAULT_JOURNAL_PATH);
    }

    public CommandJournalWriter(String journalPath) {
        try {
            this.journalPath = Paths.get(journalPath);
            Path parent = this.journalPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.writer = Files.newBufferedWriter(
                    this.journalPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize command journal writer", e);
        }
    }

    public synchronized void append(CommandJournalEntry entry) {
        try {
            writer.write(JSON.toJSONString(entry));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to append command journal entry", e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close command journal writer for {}", journalPath, e);
        }
    }
}
