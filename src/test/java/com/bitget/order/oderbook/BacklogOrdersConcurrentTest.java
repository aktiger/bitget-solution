package com.bitget.order.oderbook;

// =====================
// 4. JUnit 多线程性能测试
// 以下代码利用 Disruptor 多工作线程并发处理订单事件

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

public class BacklogOrdersConcurrentTest {

    // 与之前保持一致的转换常量
    public static final int MOD = 1_000_000_007;
    public static final int PRICE_BASE = 1_000_000_000;

    // BacklogOrdersWithTime.PRICE_BASE（可调整为 PRICE_BASE 本身）
    // 测试方法
    @Test
    public void testConcurrentPerformance() throws Exception {
        int orderCount = 1_000_000; // 测试 100 万个订单
        int[][] orders = new int[orderCount][3];
        Random rand = new Random(42);
        // 生成随机订单数据，价格 1~100,000，数量 1~10，类型随机
        for (int i = 0; i < orderCount; i++) {
            int price = rand.nextInt(100000) + 1;
            int amount = rand.nextInt(10) + 1;
            int type = rand.nextBoolean() ? 0 : 1;
            orders[i][0] = price;
            orders[i][1] = amount;
            orders[i][2] = type;
        }

        // 创建共享订单簿
        ConcurrentOrderBook orderBook = new ConcurrentOrderBook();

        // 设置 Disruptor 的 ring buffer 大小（需要为 2 的幂）
        int bufferSize = 1 << 16; // 65536
        // 创建 Disruptor
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        // 设置多个工作线程消费者，例如 4 个
        int numWorkers = 4;
        OrderWorker[] workers = new OrderWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            workers[i] = new OrderWorker(orderBook);
        }
        disruptor.handleEventsWithWorkerPool(workers);
        disruptor.start();
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();

        // 记录开始时间
        long startTime = System.nanoTime();

        // 多线程生产订单事件（这里使用多线程发布，实际 Disruptor 本身支持多生产者）
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
                        // 将订单时间设置为原始下标
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
        // 等待所有事件处理完毕
        // 暂停一会儿，等待消费者完成
        disruptor.shutdown();
        long endTime = System.nanoTime();
        long elapsedMs = (endTime - startTime) / 1_000_000;

        // 统计剩余订单数量
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
        System.out.println("Processed " + orderCount + " orders concurrently in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + total);
        System.out.println("Sample match log entries (first 10):");
        int cnt = 0;
        for (String log : orderBook.getMatchLog()) {
            System.out.println(log);
            if (++cnt >= 10) break;
        }
        // 输出最终订单簿状态
        /*System.out.println("\nFinal Buy Orders:");
        for (Map.Entry<String, OrderDisruptorQueue> entry : orderBook.getBuyTree().entrySet()) {
            int actualPrice = PRICE_BASE - Integer.parseInt(entry.getKey());
            System.out.println("Price " + actualPrice + " -> " + entry.getValue());
        }
        System.out.println("\nFinal Sell Orders:");
        for (Map.Entry<String, OrderDisruptorQueue> entry : orderBook.getSellTree().entrySet()) {
            int price = Integer.parseInt(entry.getKey());
            System.out.println("Price " + price + " -> " + entry.getValue());
        }
        assertTrue("Backlog count should be non-negative", total >= 0);*/
    }
}