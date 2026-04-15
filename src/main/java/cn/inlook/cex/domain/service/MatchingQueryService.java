package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.AccountView;
import cn.inlook.cex.domain.model.BookSnapshotView;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderRejectReason;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.OrderView;

import java.util.List;

// [ZH] 多 symbol 只读查询服务，隔离外部读取与撮合核心内部结构
// [EN] Read-only multi-symbol query service isolating external reads from matching-core internals
public class MatchingQueryService {

    private final MatchingEngineRouter router;
    private final InMemoryAccountService accountService;

    public MatchingQueryService(MatchingEngineRouter router, InMemoryAccountService accountService) {
        this.router = router;
        this.accountService = accountService;
    }

    public BookSnapshotView getBookSnapshot(String symbol) {
        MatchingEngine engine = router.getEngine(symbol);
        if (engine == null) {
            return new BookSnapshotView(symbol, null, null, List.of(), List.of());
        }

        return new BookSnapshotView(
                symbol,
                engine.getBestBidPrice(),
                engine.getBestAskPrice(),
                engine.snapshotLevels(OrderSide.BUY),
                engine.snapshotLevels(OrderSide.SELL)
        );
    }

    public OrderView getActiveOrder(String symbol, long orderId) {
        MatchingEngine engine = router.getEngine(symbol);
        if (engine == null) {
            return null;
        }

        Order order = engine.findActiveOrder(orderId);
        if (order == null) {
            return null;
        }

        return toOrderView(order, null);
    }

    public OrderView getOrder(String symbol, long orderId) {
        MatchingEngine engine = router.getEngine(symbol);
        if (engine == null) {
            return null;
        }

        Order order = engine.findOrder(orderId);
        if (order == null) {
            return null;
        }

        return toOrderView(order, engine.findRejectReason(orderId));
    }

    public AccountView getAccount(long userId) {
        Account account = accountService.getAccount(userId);
        return new AccountView(
                account.getUserId(),
                account.getAvailableCash(),
                account.getFrozenCash(),
                account.getAvailableAsset(),
                account.getFrozenAsset()
        );
    }

    private OrderView toOrderView(Order order, OrderRejectReason rejectReason) {
        return new OrderView(
                order.getOrderId(),
                order.getUserId(),
                order.getSymbol(),
                order.getSide(),
                order.getPrice(),
                order.getOriginalAmount(),
                order.getRemainingAmount(),
                order.getStatus(),
                rejectReason
        );
    }
}
