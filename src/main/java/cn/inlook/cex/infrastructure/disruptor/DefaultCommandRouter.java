package cn.inlook.cex.infrastructure.disruptor;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.PlaceOrderCommand;
import cn.inlook.cex.domain.service.MatchingEngine;
import cn.inlook.cex.domain.service.SnapshotManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCommandRouter implements CommandRouter {

    private final MatchingEngine engine;
    private final SnapshotManager snapshotManager;

    public DefaultCommandRouter(MatchingEngine engine, SnapshotManager snapshotManager) {
        this.engine = engine;
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void route(OrderEvent event, long sequence) {
        if (event.getEventType() == DisruptorEventType.PLACE_ORDER) {
            PlaceOrderCommand placeOrderCommand = event.getPlaceOrderCommand();
            if (placeOrderCommand != null) {
                engine.handle(placeOrderCommand);
                return;
            }

            Order incomingOrder = event.getOrder();
            if (incomingOrder != null) {
                engine.processOrder(incomingOrder);
            }
            return;
        }

        if (event.getEventType() == DisruptorEventType.CANCEL_ORDER) {
            CancelOrderCommand cancelOrderCommand = event.getCancelOrderCommand();
            if (cancelOrderCommand != null) {
                engine.handle(cancelOrderCommand);
                return;
            }
            engine.cancelOrder(event.getCancelOrderId());
            return;
        }

        if (event.getEventType() == DisruptorEventType.MAKE_SNAPSHOT) {
            log.info("[Matcher] Snapshot command received at sequence: {}. Halting engine to dump memory...", sequence);
            snapshotManager.saveSnapshot(engine.getActiveOrders());
            log.info("[Matcher] Snapshot dump completed. Resuming matching engine...");
        }
    }
}
