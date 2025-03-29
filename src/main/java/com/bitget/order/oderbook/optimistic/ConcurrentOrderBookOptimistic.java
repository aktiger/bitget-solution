package com.bitget.order.oderbook.optimistic;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.WorkHandler;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

class Order {
    int price;
    int amount;
    int time;
    int type; // 0: 买单, 1: 卖单

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

class OrderEvent {
    public Order order;
}

class OrderEventFactory implements EventFactory<OrderEvent> {
    @Override
    public OrderEvent newInstance() {
        return new OrderEvent();
    }
}

/**
 * OrderDisruptorQueue：基于 LMAX Disruptor 的简单 FIFO 队列
 */
class OrderDisruptorQueue {
    private static final int BUFFER_SIZE = 4096;
    private final RingBuffer<OrderEvent> ringBuffer;
    private final AtomicLong consumerSequence = new AtomicLong(-1);

    public OrderDisruptorQueue() {
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                BUFFER_SIZE,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    public void offer(Order order) {
        long seq = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(seq);
            event.order = order;
        } finally {
            ringBuffer.publish(seq);
        }
    }

    public Order peek() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        return ringBuffer.get(nextSeq).order;
    }

    public Order poll() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        Order order = ringBuffer.get(nextSeq).order;
        consumerSequence.incrementAndGet();
        return order;
    }

    public boolean isEmpty() {
        return (consumerSequence.get() + 1) > ringBuffer.getCursor();
    }

    public List<Order> toList() {
        List<Order> list = new ArrayList<>();
        long start = consumerSequence.get() + 1;
        long end = ringBuffer.getCursor();
        for (long seq = start; seq <= end; seq++) {
            list.add(ringBuffer.get(seq).order);
        }
        return list;
    }

    @Override
    public String toString() {
        return toList().toString();
    }
}

// =====================
// 2. 共享订单簿（采用 StampedLock 乐观锁方案，带有异常捕获和降级到读锁）

public class ConcurrentOrderBookOptimistic {
    // 与之前保持一致的转换常量
    public static final int MOD = 1_000_000_007;
    public static final int PRICE_BASE = 1_000_000_000;
    // ART 实现的 NavigableMap，key 为经过转换的固定宽度字符串，值为 OrderDisruptorQueue
    private final NavigableMap<String, OrderDisruptorQueue> buyTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final NavigableMap<String, OrderDisruptorQueue> sellTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    // 使用 StampedLock 替代传统锁
    private final StampedLock buyLock = new StampedLock();
    private final StampedLock sellLock = new StampedLock();
    // 用于记录匹配日志
    private final ConcurrentLinkedQueue<String> matchLog = new ConcurrentLinkedQueue<>();

    // 价格转换：保证卖单key 升序，买单key 通过 PRICE_BASE - price 实现降序
    private String sellKey(int price) {
        return String.format("%010d", price);
    }
    private String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    /**
     * 处理订单，采用乐观读 + 回退到读锁方案
     */
    public void process(Order order) {
        if (order.type == 0) { // 买单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = sellLock.tryOptimisticRead();
                Map.Entry<String, OrderDisruptorQueue> entry = null;
                try {
                    entry = sellTree.firstEntry();
                } catch (AssertionError ae) {
                    // 出现异常则回退到读锁
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
                // 升级为写锁
                stamp = sellLock.writeLock();
                try {
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
                    //matchLog.add("Match: " + order + " matched with " + sellOrder + ", quantity=" + matchQty);
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
                    OrderDisruptorQueue queue = buyTree.get(key);
                    if (queue == null) {
                        queue = new OrderDisruptorQueue();
                        buyTree.put(key, queue);
                    }
                    queue.offer(order);
                    //matchLog.add("Buy order added to backlog: " + order);
                } finally {
                    buyLock.unlockWrite(stamp);
                }
            }
        } else { // 卖单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = buyLock.tryOptimisticRead();
                Map.Entry<String, OrderDisruptorQueue> entry = null;
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
                    //matchLog.add("Match: " + buyOrder + " matched with " + order + ", quantity=" + matchQty);
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
                    OrderDisruptorQueue queue = sellTree.get(key);
                    if (queue == null) {
                        queue = new OrderDisruptorQueue();
                        sellTree.put(key, queue);
                    }
                    queue.offer(order);
                    //matchLog.add("Sell order added to backlog: " + order);
                } finally {
                    sellLock.unlockWrite(stamp);
                }
            }
        }
    }

    public NavigableMap<String, OrderDisruptorQueue> getBuyTree() {
        return buyTree;
    }

    public NavigableMap<String, OrderDisruptorQueue> getSellTree() {
        return sellTree;
    }

    public Queue<String> getMatchLog() {
        return matchLog;
    }
}

// =====================
// 3. 多线程消费者：OrderWorkerOptimistic

class OrderWorkerOptimistic implements WorkHandler<OrderEvent> {
    private final ConcurrentOrderBookOptimistic orderBook;
    public OrderWorkerOptimistic(ConcurrentOrderBookOptimistic orderBook) {
        this.orderBook = orderBook;
    }
    @Override
    public void onEvent(OrderEvent event) {
        orderBook.process(event.order);
    }
}

