package cn.inlook.cex.domain.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigInteger;

// [ZH] 极客版高性能订单模型 (Zero-GC 倾向)
// [ZH] 采用 long 替代 BigDecimal 以追求极致的内存连贯性和撮合性能。
// [ZH] 为适配 Fastjson2 恢复逻辑，移除 final 字段并提供无参构造函数。
// [EN] High-performance Order Model (Zero-GC oriented)
// [EN] Uses 'long' instead of 'BigDecimal' for extreme memory contiguity and performance.
// [EN] 'final' removed and no-args constructor added for Fastjson2 recovery.
@Getter
@Setter
@NoArgsConstructor
public class Order {

    // [ZH] 订单全局唯一 ID
    // [EN] Globally unique Order ID
    private long orderId;

    // [ZH] 用户 ID (建议网关层将 String 映射为 long，杜绝 String 开销)
    // [EN] User ID (Gateway should map String to long to eliminate String overhead)
    private long userId;

    // [ZH] 订单方向 (BUY / SELL)
    // [EN] Order direction (BUY / SELL)
    private OrderSide side;

    // [ZH] 核心价格与初始数量，使用 long 存储
    // [EN] Core price and original amount, stored as long
    private long price;
    private long originalAmount;

    // [ZH] 剩余未成交数量
    // [EN] Remaining amount to be filled
    private long remainingAmount;

    // [ZH] 纳秒级时间戳，保证“时间优先”原则
    // [EN] Nanosecond timestamp to ensure "Time Priority" principle
    private long timestamp;

    // ==========================================
    // [ZH] O(1) 撤单墓碑标记
    // [EN] O(1) Cancellation Tombstone Flag
    // ==========================================

    // [ZH] 订单是否已被撤销 (默认 false)
    // [EN] Whether the order is canceled (default false)
    private boolean isCanceled = false;

    // [ZH] 获取撤销状态
    // [EN] Get cancellation status
    public boolean isCanceled() {
        return isCanceled;
    }

    /**
     * [ZH] 业务构造函数：仅在下单接口手动创建订单时调用，包含严格校验。
     * [EN] Business constructor: Invoked during manual order placement with strict validation.
     */
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

    // [ZH] 执行撤单操作
    // [EN] Execute cancellation operation
    public void cancel() {
        this.isCanceled = true;
        // [ZH] 极其重要的安全防线：强制将剩余数量归零。
        // [ZH] 这样即使它作为"僵尸节点"在被清理前被其他指针意外访问，其可撮合量也是 0，绝对不会引发资损。
        // [EN] Crucial safety net: Force remaining amount to zero.
        // [EN] Even if this zombie node is accidentally accessed before cleanup, its matchable amount is 0, preventing financial loss.
        this.remainingAmount = 0;
    }

    @Override
    public String toString() {
        // [ZH] 英文日志，方便 ELK / Datadog 等日志监控系统进行正则解析
        // [EN] English logs for easy regex parsing in monitoring systems like ELK / Datadog
        return String.format("Order[id=%d, uid=%d, side=%s, price=%d, rem/orig=%d/%d]",
                orderId, userId, side, price, remainingAmount, originalAmount);
    }
}