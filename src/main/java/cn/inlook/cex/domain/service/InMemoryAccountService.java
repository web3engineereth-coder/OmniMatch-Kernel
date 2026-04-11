package cn.inlook.cex.domain.service;

import cn.inlook.cex.domain.model.Account;
import cn.inlook.cex.domain.model.Order;
import cn.inlook.cex.domain.model.OrderSide;
import cn.inlook.cex.domain.model.TradeEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// [ZH] 最小内存账户服务：覆盖冻结、撤单释放与成交结算
// [EN] Minimal in-memory account service covering reserve, cancel release, and trade settlement
public class InMemoryAccountService implements AccountService {

    private final Map<Long, Account> accounts = new ConcurrentHashMap<>();

    public Account createAccount(long userId, long availableCash, long availableAsset) {
        Account account = new Account(userId, availableCash, availableAsset);
        accounts.put(userId, account);
        return account;
    }

    public Account getAccount(long userId) {
        Account account = accounts.get(userId);
        if (account == null) {
            throw new IllegalArgumentException("Account not found: " + userId);
        }
        return account;
    }

    @Override
    public boolean reserveForOrder(Order order) {
        Account account = getAccount(order.getUserId());
        long reserveAmount = getReserveAmount(order);

        synchronized (account) {
            if (order.getSide() == OrderSide.BUY) {
                if (account.getAvailableCash() < reserveAmount) {
                    return false;
                }
                account.reserveCash(reserveAmount);
                return true;
            }

            if (account.getAvailableAsset() < reserveAmount) {
                return false;
            }
            account.reserveAsset(reserveAmount);
            return true;
        }
    }

    @Override
    public void releaseOnCancel(Order order) {
        Account account = getAccount(order.getUserId());
        long releaseAmount = getReserveAmount(order);

        synchronized (account) {
            if (order.getSide() == OrderSide.BUY) {
                account.releaseCash(releaseAmount);
                return;
            }
            account.releaseAsset(releaseAmount);
        }
    }

    @Override
    public void settleTrade(TradeEvent tradeEvent) {
        Account buyer = getAccount(tradeEvent.getBuyerId());
        Account seller = getAccount(tradeEvent.getSellerId());
        long cashAmount = Math.multiplyExact(tradeEvent.getPrice(), tradeEvent.getQuantity());
        long assetAmount = tradeEvent.getQuantity();

        synchronized (buyer) {
            buyer.spendFrozenCash(cashAmount);
            buyer.addAvailableAsset(assetAmount);
        }

        synchronized (seller) {
            seller.spendFrozenAsset(assetAmount);
            seller.addAvailableCash(cashAmount);
        }
    }

    private long getReserveAmount(Order order) {
        if (order.getSide() == OrderSide.BUY) {
            return Math.multiplyExact(order.getPrice(), order.getRemainingAmount());
        }
        return order.getRemainingAmount();
    }
}
