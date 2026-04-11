package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.PlaceOrderCommand;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// [ZH] 单机多交易对路由器：同一 symbol 路由到同一个撮合实例
// [EN] Single-node multi-symbol router: the same symbol always routes to the same engine
public class MatchingEngineRouter {

    private final AccountService accountService;
    private final TradeEventPublisher tradeEventPublisher;
    private final OrderCanceledEventPublisher orderCanceledEventPublisher;
    private final OrderRejectedEventPublisher orderRejectedEventPublisher;
    private final boolean enableInvariantCheck;
    private final Map<String, MatchingEngine> engines = new ConcurrentHashMap<>();

    public MatchingEngineRouter(AccountService accountService, TradeEventPublisher tradeEventPublisher) {
        this(accountService,
                tradeEventPublisher,
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                false);
    }

    public MatchingEngineRouter(AccountService accountService,
                                TradeEventPublisher tradeEventPublisher,
                                boolean enableInvariantCheck) {
        this(accountService,
                tradeEventPublisher,
                new NoopOrderCanceledEventPublisher(),
                new NoopOrderRejectedEventPublisher(),
                enableInvariantCheck);
    }

    public MatchingEngineRouter(AccountService accountService,
                                TradeEventPublisher tradeEventPublisher,
                                OrderCanceledEventPublisher orderCanceledEventPublisher,
                                OrderRejectedEventPublisher orderRejectedEventPublisher) {
        this(accountService,
                tradeEventPublisher,
                orderCanceledEventPublisher,
                orderRejectedEventPublisher,
                false);
    }

    public MatchingEngineRouter(AccountService accountService,
                                TradeEventPublisher tradeEventPublisher,
                                OrderCanceledEventPublisher orderCanceledEventPublisher,
                                OrderRejectedEventPublisher orderRejectedEventPublisher,
                                boolean enableInvariantCheck) {
        this.accountService = accountService;
        this.tradeEventPublisher = tradeEventPublisher;
        this.orderCanceledEventPublisher = orderCanceledEventPublisher;
        this.orderRejectedEventPublisher = orderRejectedEventPublisher;
        this.enableInvariantCheck = enableInvariantCheck;
    }

    public void handle(PlaceOrderCommand command) {
        resolveEngine(requiredSymbol(command.getSymbol())).handle(command);
    }

    public void handle(CancelOrderCommand command) {
        MatchingEngine engine = engines.get(requiredSymbol(command.getSymbol()));
        if (engine == null) {
            orderRejectedEventPublisher.publish(new cn.inlook.cex.domain.model.OrderRejectedEvent(
                    command.getSymbol(),
                    command.getOrderId(),
                    command.getUserId(),
                    cn.inlook.cex.domain.model.OrderRejectReason.ORDER_NOT_FOUND,
                    command.getTimestamp()));
            return;
        }
        engine.handle(command);
    }

    public void processOrder(Order order) {
        resolveEngine(requiredSymbol(order.getSymbol())).processOrder(order);
    }

    public void cancelOrder(String symbol, long orderId) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        if (engine == null) {
            return;
        }
        engine.cancelOrder(orderId);
    }

    public MatchingEngine getEngine(String symbol) {
        return engines.get(requiredSymbol(symbol));
    }

    public Long getBestBidPrice(String symbol) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        return engine == null ? null : engine.getBestBidPrice();
    }

    public Long getBestAskPrice(String symbol) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        return engine == null ? null : engine.getBestAskPrice();
    }

    public boolean hasActiveOrder(String symbol, long orderId) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        return engine != null && engine.hasActiveOrder(orderId);
    }

    public int getActiveOrderCount(String symbol) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        return engine == null ? 0 : engine.getActiveOrderCount();
    }

    public java.util.List<Long> getOrderIdsAtPrice(String symbol, OrderSide side, long price) {
        MatchingEngine engine = engines.get(requiredSymbol(symbol));
        return engine == null ? java.util.List.of() : engine.getOrderIdsAtPrice(side, price);
    }

    private MatchingEngine resolveEngine(String symbol) {
        return engines.computeIfAbsent(symbol, ignored ->
                new MatchingEngine(
                        accountService,
                        tradeEventPublisher,
                        orderCanceledEventPublisher,
                        orderRejectedEventPublisher,
                        enableInvariantCheck));
    }

    private String requiredSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol must not be blank.");
        }
        return symbol;
    }
}
