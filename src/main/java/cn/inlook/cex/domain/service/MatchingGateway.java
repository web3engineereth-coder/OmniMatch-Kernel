package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.AccountView;
import cn.inlook.cex.domain.model.BookSnapshotView;
import cn.inlook.cex.domain.model.CancelOrderCommand;
import cn.inlook.cex.domain.model.EngineEvent;
import cn.inlook.cex.domain.model.OrderView;
import cn.inlook.cex.domain.model.PlaceOrderCommand;

import java.util.List;

// [ZH] 单机版最小 gateway facade：统一下单、撤单、查询与事件读取入口
// [EN] Minimal single-node gateway facade unifying command, query, and event access
public class MatchingGateway {

    private final InMemoryAccountService accountService;
    private final InMemoryEngineEventStore eventStore;
    private final MatchingEngineRouter router;
    private final MatchingQueryService queryService;

    public MatchingGateway() {
        this(new InMemoryAccountService(), new InMemoryEngineEventStore());
    }

    public MatchingGateway(InMemoryAccountService accountService, InMemoryEngineEventStore eventStore) {
        this.accountService = accountService;
        this.eventStore = eventStore;
        this.router = new MatchingEngineRouter(accountService, eventStore);
        this.queryService = new MatchingQueryService(router, accountService);
    }

    public void createAccount(long userId, long availableCash, long availableAsset) {
        accountService.createAccount(userId, availableCash, availableAsset);
    }

    public void handle(PlaceOrderCommand command) {
        router.handle(command);
    }

    public void handle(CancelOrderCommand command) {
        router.handle(command);
    }

    public BookSnapshotView getBookSnapshot(String symbol) {
        return queryService.getBookSnapshot(symbol);
    }

    public OrderView getOrder(String symbol, long orderId) {
        return queryService.getOrder(symbol, orderId);
    }

    public OrderView getActiveOrder(String symbol, long orderId) {
        return queryService.getActiveOrder(symbol, orderId);
    }

    public AccountView getAccount(long userId) {
        return queryService.getAccount(userId);
    }

    public List<EngineEvent> getEvents() {
        return eventStore.getEvents();
    }

    public void clearEvents() {
        eventStore.clear();
    }
}
