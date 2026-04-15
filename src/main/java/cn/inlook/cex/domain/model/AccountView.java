package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 最小账户只读视图
// [EN] Minimal read-only account view
@Getter
public class AccountView {

    private final long userId;
    private final long availableCash;
    private final long frozenCash;
    private final long availableAsset;
    private final long frozenAsset;

    public AccountView(long userId,
                       long availableCash,
                       long frozenCash,
                       long availableAsset,
                       long frozenAsset) {
        this.userId = userId;
        this.availableCash = availableCash;
        this.frozenCash = frozenCash;
        this.availableAsset = availableAsset;
        this.frozenAsset = frozenAsset;
    }
}
