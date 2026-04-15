package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.EngineEvent;

// [ZH] 统一事件流发布边界
// [EN] Unified publication boundary for engine events
public interface EngineEventPublisher {

    void publish(EngineEvent event);
}
