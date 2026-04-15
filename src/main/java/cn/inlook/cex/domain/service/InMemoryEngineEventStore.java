package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.EngineEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// [ZH] 最小内存事件存储，供 gateway / demo / 测试读取最近输出事件
// [EN] Minimal in-memory event store for gateway/demo/test reads of emitted events
public class InMemoryEngineEventStore implements EngineEventPublisher {

    private final CopyOnWriteArrayList<EngineEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void publish(EngineEvent event) {
        events.add(event);
    }

    public List<EngineEvent> getEvents() {
        return new ArrayList<>(events);
    }

    public void clear() {
        events.clear();
    }
}
