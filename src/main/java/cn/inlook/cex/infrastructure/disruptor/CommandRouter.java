package cn.inlook.cex.infrastructure.disruptor;

// [ZH] 命令路由边界，保留事件驱动主线但不把分发逻辑塞进 Handler
// [EN] Command routing boundary that keeps dispatch logic out of the handler
public interface CommandRouter {

    void route(OrderEvent event, long sequence);
}
