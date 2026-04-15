package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.EngineEvent;

// [ZH] 统一事件流默认空实现
// [EN] Default no-op implementation for the unified event flow
public class NoopEngineEventPublisher implements EngineEventPublisher {

    @Override
    public void publish(EngineEvent event) {
        // Intentionally no-op.
    }
}
