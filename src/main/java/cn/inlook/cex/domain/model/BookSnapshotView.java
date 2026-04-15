package cn.inlook.cex.domain.model;

import lombok.Getter;

import java.util.List;

// [ZH] 单个交易对的订单簿只读快照
// [EN] Read-only order-book snapshot for a single symbol
@Getter
public class BookSnapshotView {

    private final String symbol;
    private final Long bestBid;
    private final Long bestAsk;
    private final List<BookLevelView> bidLevels;
    private final List<BookLevelView> askLevels;

    public BookSnapshotView(String symbol,
                            Long bestBid,
                            Long bestAsk,
                            List<BookLevelView> bidLevels,
                            List<BookLevelView> askLevels) {
        this.symbol = symbol;
        this.bestBid = bestBid;
        this.bestAsk = bestAsk;
        this.bidLevels = bidLevels;
        this.askLevels = askLevels;
    }
}
