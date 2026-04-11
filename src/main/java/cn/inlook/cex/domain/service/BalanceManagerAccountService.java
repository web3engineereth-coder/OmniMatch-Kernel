package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.TradeEvent;

// [ZH] 现有 BalanceManager 的轻量适配器
// [EN] Lightweight adapter around the existing BalanceManager
public class BalanceManagerAccountService implements AccountService {

    private final BalanceManager balanceManager;
    private final int baseCurrency;
    private final int quoteCurrency;

    public BalanceManagerAccountService(BalanceManager balanceManager, int baseCurrency, int quoteCurrency) {
        this.balanceManager = balanceManager;
        this.baseCurrency = baseCurrency;
        this.quoteCurrency = quoteCurrency;
    }

    @Override
    public void settleTrade(TradeEvent tradeEvent) {
        balanceManager.settle(
                tradeEvent.getBuyerId(),
                tradeEvent.getSellerId(),
                baseCurrency,
                quoteCurrency,
                tradeEvent.getQuantity(),
                tradeEvent.getPrice()
        );
    }

    @Override
    public void releaseOnCancel(Order order) {
        // [ZH] 第一阶段只保留边界，不实现完整冻结释放系统
        // [EN] Phase 1 keeps the boundary only, without full reservation release
    }
}
