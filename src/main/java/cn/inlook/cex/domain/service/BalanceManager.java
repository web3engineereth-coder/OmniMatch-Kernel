package cn.inlook.cex.domain.service;

import lombok.extern.slf4j.Slf4j;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * [ZH] 账务管理器 - 负责资产的冻结、扣减与平衡
 * [EN] Balance Manager - Responsible for freezing, deducting, and balancing assets
 */
@Slf4j
public class BalanceManager {

    // [ZH] 资产精度缩放因子 (假设为 10^8)
    // [EN] Asset precision scale factor (e.g., 10^8)
    private static final BigInteger SCALE_FACTOR = BigInteger.valueOf(100_000_000L);

    // [ZH] 用户ID -> (币种ID -> 余额对象)
    // [EN] UserID -> (CurrencyID -> Balance Object)
    private final ConcurrentHashMap<Long, Map<Integer, AssetBalance>> balances = new ConcurrentHashMap<>();

    /**
     * [ZH] 尝试冻结资产 (下单前必调)
     * [EN] Try to freeze assets (Must call before placing an order)
     */
    public boolean tryFreeze(long userId, int currencyId, long amount) {
        AssetBalance balance = getBalance(userId, currencyId);

        // [ZH] 锁住具体的 AssetBalance 实例，保证 Check-and-Set 原子性
        // [EN] Lock the specific AssetBalance instance to ensure Check-and-Set atomicity
        synchronized (balance) {
            if (balance.available >= amount) {
                balance.available -= amount;
                balance.frozen += amount;
                return true;
            }
        }
        log.warn("Freeze failed: Insufficient balance for User {} in Currency {}", userId, currencyId);
        return false;
    }

    /**
     * [ZH] 撮合成功后的资金结算
     * [EN] Settlement after successful matching
     */
    public void settle(long buyerId, long sellerId, int baseCurrency, int quoteCurrency, long amount, long price) {
        // [ZH] 1. 安全计算成交总价，防止 long 溢出
        // [EN] 1. Safely calculate total value to prevent long overflow
        long totalQuoteValue = calculateQuoteValue(amount, price);

        // [ZH] 2. 买家结算：扣除冻结的 Quote，增加可用的 Base
        // [EN] 2. Buyer Settlement: Deduct frozen Quote, increase available Base
        updateBalance(buyerId, quoteCurrency, 0, -totalQuoteValue); // Deduct frozen
        updateBalance(buyerId, baseCurrency, amount, 0);          // Add available

        // [ZH] 3. 卖家结算：扣除冻结的 Base，增加可用的 Quote
        // [EN] 3. Seller Settlement: Deduct frozen Base, increase available Quote
        updateBalance(sellerId, baseCurrency, 0, -amount);        // Deduct frozen
        updateBalance(sellerId, quoteCurrency, totalQuoteValue, 0); // Add available

        // [ZH] TODO: 此处应发送异步消息记录 Journal Entry (审计流水)
        // [EN] TODO: Send async message to record Journal Entry (Audit log)
    }

    /**
     * [ZH] 内部工具：安全修改余额
     * [EN] Internal helper: Safely update balance
     */
    private void updateBalance(long userId, int currencyId, long availableDelta, long frozenDelta) {
        AssetBalance balance = getBalance(userId, currencyId);
        synchronized (balance) {
            balance.available += availableDelta;
            balance.frozen += frozenDelta;
        }
    }

    /**
     * [ZH] 使用 BigInteger 计算并缩放价格，防止 18 位精度下的 long 乘法溢出
     * [EN] Calculate and scale price using BigInteger to prevent long multiplication overflow
     */
    private long calculateQuoteValue(long amount, long price) {
        return BigInteger.valueOf(amount)
                .multiply(BigInteger.valueOf(price))
                .divide(SCALE_FACTOR)
                .longValue();
    }

    private AssetBalance getBalance(long userId, int currencyId) {
        return balances.computeIfAbsent(userId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(currencyId, k -> new AssetBalance());
    }

    // [ZH] 内部资产类
    // [EN] Internal Asset Class
    private static class AssetBalance {
        long available = 0;
        long frozen = 0;
    }
}