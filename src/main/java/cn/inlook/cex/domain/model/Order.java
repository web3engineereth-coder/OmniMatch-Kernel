package cn.inlook.cex.domain.model;

import lombok.Getter;
import java.math.BigInteger;

// [ZH] 极客版高性能订单模型 (Zero-GC 倾向)
// [ZH] 采用 long 替代 BigDecimal 以追求极致的内存连贯性和撮合性能。
// [EN] High-performance Order Model (Zero-GC oriented)
// [EN] Uses 'long' instead of 'BigDecimal' to achieve extreme memory contiguity and matching performance.
@Getter
public class Order {

    // [ZH] 订单全局唯一 ID
    // [EN] Globally unique Order ID
    private final long orderId;

    // [ZH] 用户 ID (建议网关层将 String 映射为 long，杜绝 String 开销)
    // [EN] User ID (Gateway should map String to long to eliminate String overhead)
    private final long userId;

    // [ZH] 订单方向 (BUY / SELL)
    // [EN] Order direction (BUY / SELL)
    private final OrderSide side;

    // [ZH] 核心价格与初始数量，使用 long 存储
    // [EN] Core price and original amount, stored as long
    private final long price;
    private final long originalAmount;

    // [ZH] 剩余未成交数量
    // [EN] Remaining amount to be filled
    private long remainingAmount;

    // [ZH] 纳秒级时间戳，保证“时间优先”原则
    // [EN] Nanosecond timestamp to ensure "Time Priority" principle
    private final long timestamp;

    public Order(long orderId, long userId, OrderSide side, long price, long amount) {
        if (price <= 0 || amount <= 0) {
            // Exceptions MUST be in English
            throw new IllegalArgumentException("Price and amount must be strictly positive.");
        }
        this.orderId = orderId;
        this.userId = userId;
        this.side = side;
        this.price = price;
        this.originalAmount = amount;
        this.remainingAmount = amount;
        this.timestamp = System.nanoTime();
    }

    // [ZH] 判断订单是否已经完全成交
    // [EN] Check if the order has been completely filled
    public boolean isFilled() {
        return remainingAmount <= 0;
    }

    // [ZH] 执行成交扣减逻辑
    // [EN] Execute the logic to deduct filled amount
    // @param tradedAmount [ZH] 本次撮合的成交量 / [EN] The amount filled in this match
    public void fill(long tradedAmount) {
        if (tradedAmount <= 0) {
            throw new IllegalArgumentException("Traded amount must be greater than 0.");
        }
        if (tradedAmount > remainingAmount) {
            throw new IllegalStateException("Traded amount exceeds remaining amount.");
        }
        this.remainingAmount -= tradedAmount;
    }

    // [ZH] 安全计算本次成交的总价值（单价 * 数量）
    // [ZH] 关键退化点：使用 BigInteger 防止两个 18 位精度的 long 相乘直接导致溢出
    // [EN] Safely calculate the total value of this trade (price * amount)
    // [EN] Crucial degradation point: Use BigInteger to prevent overflow when multiplying two 18-precision longs
    public BigInteger calculateTradeValue(long tradedAmount) {
        // [ZH] 退化为 BigInteger 确保财务绝对安全，该方法仅在生成资金流水或最终结算时调用
        // [EN] Degrade to BigInteger to ensure absolute financial safety. Only called during settlement or journal entry generation.
        BigInteger p = BigInteger.valueOf(this.price);
        BigInteger a = BigInteger.valueOf(tradedAmount);
        return p.multiply(a);
    }

    @Override
    public String toString() {
        // [ZH] 英文日志，方便 ELK / Datadog 等日志监控系统进行正则解析
        // [EN] English logs for easy regex parsing in monitoring systems like ELK / Datadog
        return String.format("Order[id=%d, uid=%d, side=%s, price=%d, rem/orig=%d/%d]",
                orderId, userId, side, price, remainingAmount, originalAmount);
    }
}