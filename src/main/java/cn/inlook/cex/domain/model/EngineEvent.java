package cn.inlook.cex.domain.model;

// [ZH] 撮合核心统一输出事件边界
// [EN] Unified output event boundary for the matching core
public interface EngineEvent {

    EngineEventType getEventType();

    String getSymbol();

    long getTimestamp();
}
