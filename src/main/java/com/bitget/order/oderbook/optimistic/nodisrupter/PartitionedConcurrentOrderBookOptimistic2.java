package com.bitget.order.oderbook.optimistic.nodisrupter;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.*;
import java.util.concurrent.*;
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
// 2. 单个分区的订单簿（采用 ART 存储，每个价格对应一个 ConcurrentLinkedQueue<Order>）
//    使用乐观锁（StampedLock）保护每棵树
class ConcurrentOrderBookOptimisticSimple {

    private static final int MOD = 1_000_000_007;
    // 为了转换买单价格，我们假设订单价格不会超过 10^9
    private static final int PRICE_BASE = 1_000_000_000;

    // 使用 ART 实现的 NavigableMap 保存订单队列（同一价格下按 FIFO 排序）
    private final NavigableMap<String, ConcurrentLinkedQueue<Order>> buyTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final NavigableMap<String, ConcurrentLinkedQueue<Order>> sellTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final StampedLock buyLock = new StampedLock();
    private final StampedLock sellLock = new StampedLock();
    private final List<String> matchLog = Collections.synchronizedList(new ArrayList<>());

    // 价格转换：卖单 key 为固定宽度字符串（升序），买单 key 为 (PRICE_BASE - price) 格式化后的字符串
    private String sellKey(int price) {
        return String.format("%010d", price);
    }
    private String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    // 处理订单（内部匹配过程与之前类似）
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
                    // 重读
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
                    //matchLog.add("Match: " + order + " matched with " + sellOrder + ", qty=" + matchQty);
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
                    //matchLog.add("Buy order added: " + order);
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
                int actualBuyPrice = PRICE_BASE - transformed;
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
                    actualBuyPrice = PRICE_BASE - transformed;
                    if (actualBuyPrice < order.price) {
                        continueMatching = false;
                        break;
                    }
                    buyOrder = entry.getValue().peek();
                    if (buyOrder == null) continue;
                    int matchQty = Math.min(order.amount, buyOrder.amount);
                    //matchLog.add("Match: " + buyOrder + " matched with " + order + ", qty=" + matchQty);
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
                    //matchLog.add("Sell order added: " + order);
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
        return matchLog;
    }
}

// ----------------------
// 3. 分区订单簿：将订单簿按价格分区（例如128个分区），各自独立处理，最后合并结果
public class PartitionedConcurrentOrderBookOptimistic2 {
    private final int partitions;
    private final ConcurrentOrderBookOptimisticSimple[] orderBooks;

    public PartitionedConcurrentOrderBookOptimistic2(int partitions) {
        this.partitions = partitions;
        orderBooks = new ConcurrentOrderBookOptimisticSimple[partitions];
        for (int i = 0; i < partitions; i++) {
            orderBooks[i] = new ConcurrentOrderBookOptimisticSimple();
        }
    }

    // 简单取模分区（可以根据业务调整）
    private int getPartition(Order order) {
        return order.price % partitions;
    }

    public void process(Order order) {
        int idx = getPartition(order);
        orderBooks[idx].process(order);
    }

    public List<String> getMatchLogs() {
        List<String> logs = new ArrayList<>();
        for (ConcurrentOrderBookOptimisticSimple book : orderBooks) {
            logs.addAll(book.getMatchLog());
        }
        return logs;
    }

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
// 4. 多线程消费者：使用分区订单簿的工作线程
class OrderWorkerOptimisticPartitioned2 implements Runnable {
    private final PartitionedConcurrentOrderBookOptimistic2 orderBook;
    private final BlockingQueue<Order> orderQueue; // 每个工作线程从共享订单队列取订单
    public OrderWorkerOptimisticPartitioned2(PartitionedConcurrentOrderBookOptimistic2 orderBook,
                                             BlockingQueue<Order> orderQueue) {
        this.orderBook = orderBook;
        this.orderQueue = orderQueue;
    }
    @Override
    public void run() {
        try {
            Order order;
            while ((order = orderQueue.poll(100, TimeUnit.MILLISECONDS)) != null) {
                orderBook.process(order);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}