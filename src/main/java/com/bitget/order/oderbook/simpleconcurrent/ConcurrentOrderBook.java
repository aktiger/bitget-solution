package com.bitget.order.oderbook.simpleconcurrent;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.*;
import java.util.concurrent.*;


// =====================
// 1. 数据结构与基础队列实现

// 订单类：包含价格、数量、time（下单时间，下标越小表示越早）、type（0:买单，1:卖单）




/*
class OrderEventFactory implements EventFactory<OrderEvent> {
    @Override
    public OrderEvent newInstance() {
        return new OrderEvent();
    }
}*/

/**
 * OrderDisruptorQueue：基于 Disruptor RingBuffer 实现的简单 FIFO 队列。
 * 注意：为了演示，这里实现了 offer/peek/poll/isEmpty/toList 方法，假设只有单线程消费此队列。
 */
/*class OrderDisruptorQueue {
    private static final int BUFFER_SIZE = 4096;
    private final RingBuffer<OrderEvent> ringBuffer;
    // 消费者当前消费到的序列，初始为 -1
    private final AtomicLong consumerSequence = new AtomicLong(-1);

    public OrderDisruptorQueue() {
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                BUFFER_SIZE,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        // 我们不设置实际的 EventHandler，因为队列操作由直接访问 RingBuffer 完成
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    // 将订单添加到队列中
    public void offer(Order order) {
        long seq = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(seq);
            event.order = order;
        } finally {
            ringBuffer.publish(seq);
        }
    }

    // 获取队首元素但不删除
    public Order peek() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        OrderEvent event = ringBuffer.get(nextSeq);
        return event.order;
    }

    // 移除并返回队首元素
    public Order poll() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        OrderEvent event = ringBuffer.get(nextSeq);
        consumerSequence.incrementAndGet();
        return event.order;
    }

    // 检查队列是否为空
    public boolean isEmpty() {
        return (consumerSequence.get() + 1) > ringBuffer.getCursor();
    }

    // 返回队列中的所有订单（仅用于调试）
    public List<Order> toList() {
        List<Order> list = new ArrayList<>();
        long start = consumerSequence.get() + 1;
        long end = ringBuffer.getCursor();
        for (long seq = start; seq <= end; seq++) {
            OrderEvent event = ringBuffer.get(seq);
            list.add(event.order);
        }
        return list;
    }

    @Override
    public String toString() {
        return toList().toString();
    }
}*/

// =====================
// 2. 共享订单簿 ConcurrentOrderBook（使用 ART 实现的 NavigableMap 保存订单队列）


class ConcurrentOrderBook {

    // 模数
    private static final int MOD = 1_000_000_007;
    // 假设订单价格不会超过 10^9
    private static final int PRICE_BASE = 1_000_000_000;

    // ART 实现的 NavigableMap，键为经过价格转换的固定宽度字符串，值为 OrderDisruptorQueue
    private final NavigableMap<String, OrderDisruptorQueue> buyTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final NavigableMap<String, OrderDisruptorQueue> sellTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    // 分别对买单和卖单树加锁
    private final Object buyLock = new Object();
    private final Object sellLock = new Object();
    // 用于记录匹配日志，线程安全
    private final ConcurrentLinkedQueue<String> matchLog = new ConcurrentLinkedQueue<>();

    // 辅助转换函数，与前面相同
    private String sellKey(int price) {
        return String.format("%010d", price);
    }
    private String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    /**
     * 处理一个订单：
     * - 如果是买单，尝试匹配卖单（取卖单树中最低价格且 sellPrice <= order.price），否则加入买单树；
     * - 如果是卖单，尝试匹配买单（取买单树中最高买价且 actualBuyPrice >= order.price），否则加入卖单树。
     * 匹配时同一价格队列使用 FIFO（OrderDisruptorQueue）保证时间早的优先。
     */
    public void process(Order order) {
        if (order.type == 0) { // 买单
            // 匹配卖单部分：对卖单树加锁
            synchronized(sellLock) {
                while (order.amount > 0 && !sellTree.isEmpty()) {
                    Map.Entry<String, OrderDisruptorQueue> entry = sellTree.firstEntry();
                    int sellPrice = Integer.parseInt(entry.getKey());
                    if (sellPrice > order.price) break;
                    Order sellOrder = entry.getValue().peek();
                    if (sellOrder == null) break;
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
                }
            }
            if (order.amount > 0) {
                synchronized(buyLock) {
                    String key = buyKey(order.price);
                    OrderDisruptorQueue queue = buyTree.get(key);
                    if (queue == null) {
                        queue = new OrderDisruptorQueue();
                        buyTree.put(key, queue);
                    }
                    queue.offer(order);
                    //matchLog.add("Buy order added to backlog: " + order);
                }
            }
        } else { // 卖单
            synchronized(buyLock) {
                while (order.amount > 0 && !buyTree.isEmpty()) {
                    Map.Entry<String, OrderDisruptorQueue> entry = buyTree.firstEntry();
                    int transformed = Integer.parseInt(entry.getKey());
                    int actualBuyPrice = PRICE_BASE - transformed;
                    if (actualBuyPrice < order.price) break;
                    Order buyOrder = entry.getValue().peek();
                    if (buyOrder == null) break;
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
                }
            }
            if (order.amount > 0) {
                synchronized(sellLock) {
                    String key = sellKey(order.price);
                    OrderDisruptorQueue queue = sellTree.get(key);
                    if (queue == null) {
                        queue = new OrderDisruptorQueue();
                        sellTree.put(key, queue);
                    }
                    queue.offer(order);
                    //matchLog.add("Sell order added to backlog: " + order);
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

