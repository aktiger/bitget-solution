package com.bitget.order.oderbook;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.StampedLock;

// =====================
// 1. 基础数据结构和队列实现






/**
 * OrderDisruptorQueue：基于 LMAX Disruptor 的简单 FIFO 队列实现
 */
/*
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
        // 本队列仅用于直接访问 RingBuffer，不设置 EventHandler
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
*/

// =====================
// 2. 共享订单簿（使用 ART 实现的 NavigableMap 和 StampedLock 实现乐观锁）

class ConcurrentOrderBookOptimistic {
    // 模数
    private static final int MOD = 1_000_000_007;
    // 假设订单价格不会超过 10^9
    private static final int PRICE_BASE = 1_000_000_000;


    // ART 实现的 NavigableMap，key 为经过转换的固定宽度字符串，值为 OrderDisruptorQueue
    private final NavigableMap<String, OrderDisruptorQueue> buyTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    private final NavigableMap<String, OrderDisruptorQueue> sellTree =
            new AdaptiveRadixTree<>(BinaryComparables.forString());
    // 使用 StampedLock 替代传统锁
    private final StampedLock buyLock = new StampedLock();
    private final StampedLock sellLock = new StampedLock();
    // 使用线程安全的队列记录匹配日志
    private final ConcurrentLinkedQueue<String> matchLog = new ConcurrentLinkedQueue<>();

    // 与之前一致的价格转换函数
    private String sellKey(int price) {
        return String.format("%010d", price);
    }
    private String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    /**
     * 处理订单：
     * 采用乐观读模式（StampedLock.tryOptimisticRead）读取后，
     * 在需要修改时升级为写锁，并在写锁下重查条件后修改数据结构。
     */
    public void process(Order order) {
        if (order.type == 0) { // 买单
            // 匹配卖单：在 sellTree 中找出最低卖价且 <= order.price 的订单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = sellLock.tryOptimisticRead();
                Map.Entry<String, OrderDisruptorQueue> entry = sellTree.firstEntry();
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
                // 检查无冲突后升级为写锁
                stamp = sellLock.writeLock();
                try {
                    // 重新读取
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
                    matchLog.add("Match: " + order + " matched with " + sellOrder + ", quantity=" + matchQty);
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
                    matchLog.add("Buy order added to backlog: " + order);
                } finally {
                    buyLock.unlockWrite(stamp);
                }
            }
        } else { // 卖单
            boolean continueMatching = true;
            while (order.amount > 0 && continueMatching) {
                long stamp = buyLock.tryOptimisticRead();
                Map.Entry<String, OrderDisruptorQueue> entry = buyTree.firstEntry();
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
                    matchLog.add("Match: " + buyOrder + " matched with " + order + ", quantity=" + matchQty);
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
                    matchLog.add("Sell order added to backlog: " + order);
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
// 3. 多线程消费者：OrderWorker，实现 WorkHandler<OrderEvent>
/*class OrderWorker implements WorkHandler<OrderEvent> {
    private final ConcurrentOrderBook orderBook;
    public OrderWorker(ConcurrentOrderBook orderBook) {
        this.orderBook = orderBook;
    }
    @Override
    public void onEvent(OrderEvent event) {
        // 处理当前订单事件
        orderBook.process(event.order);
    }
}*/

// =====================
// 4. JUnit 多线程性能测试代码（使用 Disruptor 多工作线程并发处理订单事件）

/*public class BacklogOrdersConcurrentOptimisticTest {

    public static final int MOD = 1_000_000_007;
    public static final int PRICE_BASE = 1_000_000_000;

    @Test
    public void testConcurrentOptimisticPerformance() throws Exception {
        int orderCount = 1_000_000;  // 100 万订单
        int[][] orders = new int[orderCount][3];
        Random rand = new Random(42);
        for (int i = 0; i < orderCount; i++) {
            int price = rand.nextInt(100000) + 1;
            int amount = rand.nextInt(10) + 1;
            int type = rand.nextBoolean() ? 0 : 1;
            orders[i][0] = price;
            orders[i][1] = amount;
            orders[i][2] = type;
        }

        ConcurrentOrderBook orderBook = new ConcurrentOrderBook();

        int bufferSize = 1 << 16; // 65536
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        int numWorkers = 4;
        OrderWorker[] workers = new OrderWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            workers[i] = new OrderWorker(orderBook);
        }
        disruptor.handleEventsWithWorkerPool(workers);
        disruptor.start();
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();

        long startTime = System.nanoTime();
        ExecutorService producerPool = Executors.newFixedThreadPool(4);
        int ordersPerThread = orderCount / 4;
        CountDownLatch latch = new CountDownLatch(4);
        for (int t = 0; t < 4; t++) {
            final int startIdx = t * ordersPerThread;
            final int endIdx = (t == 3) ? orderCount : startIdx + ordersPerThread;
            producerPool.submit(() -> {
                for (int i = startIdx; i < endIdx; i++) {
                    long seq = ringBuffer.next();
                    try {
                        OrderEvent event = ringBuffer.get(seq);
                        event.order = new Order(orders[i][0], orders[i][1], i, orders[i][2]);
                    } finally {
                        ringBuffer.publish(seq);
                    }
                }
                latch.countDown();
            });
        }
        latch.await();
        producerPool.shutdown();
        disruptor.shutdown();
        long endTime = System.nanoTime();
        long elapsedMs = (endTime - startTime) / 1_000_000;

        long total = 0;
        for (OrderDisruptorQueue queue : orderBook.getBuyTree().values()) {
            for (Order o : queue.toList()) {
                total = (total + o.amount) % MOD;
            }
        }
        for (OrderDisruptorQueue queue : orderBook.getSellTree().values()) {
            for (Order o : queue.toList()) {
                total = (total + o.amount) % MOD;
            }
        }
        System.out.println("Processed " + orderCount + " orders concurrently (optimistic) in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + total);
        System.out.println("Sample match log entries (first 10):");
        int cnt = 0;
        for (String log : orderBook.getMatchLog()) {
            System.out.println(log);
            if (++cnt >= 10) break;
        }
        System.out.println("\nFinal Buy Orders:");
        for (Map.Entry<String, OrderDisruptorQueue> entry : orderBook.getBuyTree().entrySet()) {
            int actualPrice = PRICE_BASE - Integer.parseInt(entry.getKey());
            System.out.println("Price " + actualPrice + " -> " + entry.getValue());
        }
        System.out.println("\nFinal Sell Orders:");
        for (Map.Entry<String, OrderDisruptorQueue> entry : orderBook.getSellTree().entrySet()) {
            int price = Integer.parseInt(entry.getKey());
            System.out.println("Price " + price + " -> " + entry.getValue());
        }
        assertTrue("Backlog count should be non-negative", total >= 0);
    }
}*/
