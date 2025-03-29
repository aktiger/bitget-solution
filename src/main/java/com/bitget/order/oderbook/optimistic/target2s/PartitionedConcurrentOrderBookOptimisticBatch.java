package com.bitget.order.oderbook.optimistic.target2s;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.StampedLock;

// ----------------------
// 1. 订单数据结构
class Order {
    int price;
    int amount;
    int time; // 下单顺序，数值越小越早
    int type; // 0: Buy, 1: Sell

    public Order(int price, int amount, int time, int type) {
        this.price = price;
        this.amount = amount;
        this.time = time;
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("%s(price=%d, amount=%d, time=%d)",
                type == 0 ? "Buy" : "Sell", price, amount, time);
    }
}

// ----------------------
// 2. 单个分区的订单簿：ConcurrentOrderBookOptimisticSimple
//    使用 ART 存储，同一价格下使用 ConcurrentLinkedQueue<Order>
//    采用 StampedLock 进行保护；日志记录可以关闭以提高性能
class ConcurrentOrderBookOptimisticSimple {
    private final NavigableMap<String, ConcurrentLinkedQueue<Order>> buyTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final NavigableMap<String, ConcurrentLinkedQueue<Order>> sellTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final StampedLock buyLock = new StampedLock();
    private final StampedLock sellLock = new StampedLock();
    // 生产环境下建议关闭日志记录以减少开销，此处调试时可启用
    // private final List<String> matchLog = Collections.synchronizedList(new ArrayList<>());

    // 价格转换：卖单key为固定宽度字符串（升序），买单key为 (PRICE_BASE - price) 格式化后的字符串
    private String sellKey(int price) {
        return String.format("%010d", price);
    }
    private String buyKey(int price) {
        return String.format("%010d", BacklogOrdersWithTime.PRICE_BASE - price);
    }

    /**
     * 处理订单：对买单和卖单分别匹配对手订单，并将未成交订单存入对应树中。
     */
    public void process(Order order) {
        if (order.type == 0) { // 买单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = sellLock.tryOptimisticRead();
                Map.Entry<String, ConcurrentLinkedQueue<Order>> entry = null;
                try {
                    entry = sellTree.firstEntry();
                } catch (AssertionError ae) {
                    stamp = sellLock.readLock();
                    try {
                        entry = sellTree.firstEntry();
                    } finally {
                        sellLock.unlockRead(stamp);
                    }
                }
                if (entry == null) break;
                int sellPrice = Integer.parseInt(entry.getKey());
                if (sellPrice > order.price) {
                    if (!sellLock.validate(stamp)) continue;
                    break;
                }
                Order sellOrder = entry.getValue().peek();
                if (sellOrder == null) {
                    if (!sellLock.validate(stamp)) continue;
                    break;
                }
                stamp = sellLock.writeLock();
                try {
                    // 重读以保证数据一致
                    entry = sellTree.firstEntry();
                    if (entry == null) continue;
                    sellPrice = Integer.parseInt(entry.getKey());
                    if (sellPrice > order.price) {
                        continueMatching = false;
                        break;
                    }
                    sellOrder = entry.getValue().peek();
                    if (sellOrder == null) continue;
                    int matchQty = Math.min(order.amount, sellOrder.amount);
                    // 此处可关闭日志记录：matchLog.add(...);
                    order.amount -= matchQty;
                    sellOrder.amount -= matchQty;
                    if (sellOrder.amount == 0) {
                        entry.getValue().poll();
                        if (entry.getValue().isEmpty()) {
                            sellTree.pollFirstEntry();
                        }
                    }
                } finally {
                    sellLock.unlockWrite(stamp);
                }
            }
            if (order.amount > 0) {
                long stamp = buyLock.writeLock();
                try {
                    String key = buyKey(order.price);
                    ConcurrentLinkedQueue<Order> queue = buyTree.get(key);
                    if (queue == null) {
                        queue = new ConcurrentLinkedQueue<>();
                        buyTree.put(key, queue);
                    }
                    queue.offer(order);
                    // matchLog.add("Buy order added: " + order);
                } finally {
                    buyLock.unlockWrite(stamp);
                }
            }
        } else { // 卖单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = buyLock.tryOptimisticRead();
                Map.Entry<String, ConcurrentLinkedQueue<Order>> entry = null;
                try {
                    entry = buyTree.firstEntry();
                } catch (AssertionError ae) {
                    stamp = buyLock.readLock();
                    try {
                        entry = buyTree.firstEntry();
                    } finally {
                        buyLock.unlockRead(stamp);
                    }
                }
                if (entry == null) break;
                int transformed = Integer.parseInt(entry.getKey());
                int actualBuyPrice = BacklogOrdersWithTime.PRICE_BASE - transformed;
                if (actualBuyPrice < order.price) {
                    if (!buyLock.validate(stamp)) continue;
                    break;
                }
                Order buyOrder = entry.getValue().peek();
                if (buyOrder == null) {
                    if (!buyLock.validate(stamp)) continue;
                    break;
                }
                stamp = buyLock.writeLock();
                try {
                    entry = buyTree.firstEntry();
                    if (entry == null) continue;
                    transformed = Integer.parseInt(entry.getKey());
                    actualBuyPrice = BacklogOrdersWithTime.PRICE_BASE - transformed;
                    if (actualBuyPrice < order.price) {
                        continueMatching = false;
                        break;
                    }
                    buyOrder = entry.getValue().peek();
                    if (buyOrder == null) continue;
                    int matchQty = Math.min(order.amount, buyOrder.amount);
                    // matchLog.add("Match: " + buyOrder + " with " + order + ", qty=" + matchQty);
                    order.amount -= matchQty;
                    buyOrder.amount -= matchQty;
                    if (buyOrder.amount == 0) {
                        entry.getValue().poll();
                        if (entry.getValue().isEmpty()) {
                            buyTree.pollFirstEntry();
                        }
                    }
                } finally {
                    buyLock.unlockWrite(stamp);
                }
            }
            if (order.amount > 0) {
                long stamp = sellLock.writeLock();
                try {
                    String key = sellKey(order.price);
                    ConcurrentLinkedQueue<Order> queue = sellTree.get(key);
                    if (queue == null) {
                        queue = new ConcurrentLinkedQueue<>();
                        sellTree.put(key, queue);
                    }
                    queue.offer(order);
                    // matchLog.add("Sell order added: " + order);
                } finally {
                    sellLock.unlockWrite(stamp);
                }
            }
        }
    }

    public NavigableMap<String, ConcurrentLinkedQueue<Order>> getBuyTree() {
        return buyTree;
    }
    public NavigableMap<String, ConcurrentLinkedQueue<Order>> getSellTree() {
        return sellTree;
    }
    public List<String> getMatchLog() {
        return Collections.emptyList(); // 此处关闭日志记录以提高性能
    }
}

// ----------------------
// 3. 分区订单簿：PartitionedConcurrentOrderBookOptimisticBatch
//    将订单簿分为多个独立分区，每个分区使用一个 ConcurrentOrderBookOptimisticSimple
class PartitionedConcurrentOrderBookOptimisticBatch {
    private final int partitions;
    private final ConcurrentOrderBookOptimisticSimple[] orderBooks;

    public PartitionedConcurrentOrderBookOptimisticBatch(int partitions) {
        this.partitions = partitions;
        orderBooks = new ConcurrentOrderBookOptimisticSimple[partitions];
        for (int i = 0; i < partitions; i++) {
            orderBooks[i] = new ConcurrentOrderBookOptimisticSimple();
        }
    }

    // 根据价格进行分区（简单使用 price mod partitions）
    private int getPartition(Order order) {
        return order.price % partitions;
    }

    public void process(Order order) {
        int idx = getPartition(order);
        orderBooks[idx].process(order);
    }

    // 合并所有分区剩余订单数量
    public long getBacklogCount() {
        long total = 0;
        for (ConcurrentOrderBookOptimisticSimple book : orderBooks) {
            for (ConcurrentLinkedQueue<Order> queue : book.getBuyTree().values()) {
                for (Order o : queue) {
                    total += o.amount;
                }
            }
            for (ConcurrentLinkedQueue<Order> queue : book.getSellTree().values()) {
                for (Order o : queue) {
                    total += o.amount;
                }
            }
        }
        return total;
    }
}

// ----------------------
// 4. 测试入口：采用 BlockingQueue + 多线程批处理，目标500ms以内（优化到500ms甚至更低）
class BacklogOrdersWithTime {
    public static final int PRICE_BASE = 1_000_000_000;
}