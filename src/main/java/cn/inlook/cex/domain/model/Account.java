package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 单市场阶段的最小账户模型
// [EN] Minimal account model for the single-market phase
@Getter
public class Account {

    private final long userId;
    private long availableCash;
    private long frozenCash;
    private long availableAsset;
    private long frozenAsset;

    public Account(long userId, long availableCash, long availableAsset) {
        this.userId = userId;
        this.availableCash = availableCash;
        this.availableAsset = availableAsset;
    }

    public void reserveCash(long amount) {
        ensureNonNegative(amount);
        if (availableCash < amount) {
            throw new IllegalStateException("Insufficient available cash.");
        }
        availableCash -= amount;
        frozenCash += amount;
    }

    public void releaseCash(long amount) {
        ensureNonNegative(amount);
        if (frozenCash < amount) {
            throw new IllegalStateException("Frozen cash underflow.");
        }
        frozenCash -= amount;
        availableCash += amount;
    }

    public void spendFrozenCash(long amount) {
        ensureNonNegative(amount);
        if (frozenCash < amount) {
            throw new IllegalStateException("Frozen cash underflow.");
        }
        frozenCash -= amount;
    }

    public void reserveAsset(long amount) {
        ensureNonNegative(amount);
        if (availableAsset < amount) {
            throw new IllegalStateException("Insufficient available asset.");
        }
        availableAsset -= amount;
        frozenAsset += amount;
    }

    public void releaseAsset(long amount) {
        ensureNonNegative(amount);
        if (frozenAsset < amount) {
            throw new IllegalStateException("Frozen asset underflow.");
        }
        frozenAsset -= amount;
        availableAsset += amount;
    }

    public void spendFrozenAsset(long amount) {
        ensureNonNegative(amount);
        if (frozenAsset < amount) {
            throw new IllegalStateException("Frozen asset underflow.");
        }
        frozenAsset -= amount;
    }

    public void addAvailableCash(long amount) {
        ensureNonNegative(amount);
        availableCash += amount;
    }

    public void addAvailableAsset(long amount) {
        ensureNonNegative(amount);
        availableAsset += amount;
    }

    private void ensureNonNegative(long amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("Amount must not be negative.");
        }
    }
}
