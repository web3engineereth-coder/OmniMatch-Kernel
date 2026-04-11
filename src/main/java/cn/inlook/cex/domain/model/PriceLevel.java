package cn.inlook.cex.domain.model;

import lombok.Getter;

// [ZH] 单个价格档位，内部维护 FIFO 双向链表
// [EN] Single price level with an internal FIFO doubly linked list
@Getter
public class PriceLevel {

    private final long price;
    private OrderNode head;
    private OrderNode tail;
    private int size;

    public PriceLevel(long price) {
        this.price = price;
    }

    public void append(OrderNode node) {
        node.setPriceLevel(this);
        node.setPrev(tail);
        node.setNext(null);

        if (tail != null) {
            tail.setNext(node);
        } else {
            head = node;
        }

        tail = node;
        size++;
    }

    public void unlink(OrderNode node) {
        OrderNode prev = node.getPrev();
        OrderNode next = node.getNext();

        if (prev != null) {
            prev.setNext(next);
        } else {
            head = next;
        }

        if (next != null) {
            next.setPrev(prev);
        } else {
            tail = prev;
        }

        node.setPrev(null);
        node.setNext(null);
        node.setPriceLevel(null);
        size--;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
