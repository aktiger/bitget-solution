package com.bitget.order.oderbook;

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

public class BacklogOrdersConcurrentOptimisticTest {

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

        ConcurrentOrderBookOptimistic orderBook = new ConcurrentOrderBookOptimistic();

        int bufferSize = 1 << 16; // 65536
        Disruptor<OrderEvent> disruptor;
        disruptor = new Disruptor<>(
                new OrderEventFactory(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        int numWorkers = 4;
        OrderWorkerOptimistic[] workers = new OrderWorkerOptimistic[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            workers[i] = new OrderWorkerOptimistic(orderBook);
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

        /*int cnt = 0;
        for (String log : orderBook.getMatchLog()) {
            System.out.println(log);
            if (++cnt >= 10) break;
        }
        System.out.println("\nFinal Buy Orders:");
        orderBook.getBuyTree().entrySet().forEach(entry -> {
            int actualPrice = PRICE_BASE - Integer.parseInt(entry.getKey());
            System.out.println("Price " + actualPrice + " -> " + entry.getValue());
        });
        System.out.println("\nFinal Sell Orders:");
        for (Map.Entry<String, OrderDisruptorQueue> entry : orderBook.getSellTree().entrySet()) {
            int price = Integer.parseInt(entry.getKey());
            System.out.println("Price " + price + " -> " + entry.getValue());
        }
        assertTrue("Backlog count should be non-negative", total >= 0);*/
    }
}