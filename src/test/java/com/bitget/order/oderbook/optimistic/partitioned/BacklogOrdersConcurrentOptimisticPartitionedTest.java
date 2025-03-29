package com.bitget.order.oderbook.optimistic.partitioned;



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


// 5. JUnit 多线程性能测试代码（分区版本）
public class BacklogOrdersConcurrentOptimisticPartitionedTest {
    public static final int MOD = 1_000_000_007;
    public static final int PRICE_BASE = 1_000_000_000;

    @Test
    public void testConcurrentOptimisticPartitionedPerformance() throws Exception {
        int orderCount = 1_000_000;
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

        // 分区数，根据机器核数合理选择（比如 16 个分区）
        int partitions = 16;
        PartitionedConcurrentOrderBookOptimistic partitionedBook =
                new PartitionedConcurrentOrderBookOptimistic(partitions);

        int bufferSize = 1 << 16; // 65536
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        int numWorkers = 4;
        OrderWorkerOptimisticPartitioned[] workers = new OrderWorkerOptimisticPartitioned[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            workers[i] = new OrderWorkerOptimisticPartitioned(partitionedBook);
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

        long total = partitionedBook.getBacklogCount();
        System.out.println("Processed " + orderCount + " orders concurrently (partitioned optimistic) in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + total);
        System.out.println("Sample match log entries (first 10):");
        /*int cnt = 0;
        for (String log : partitionedBook.getMatchLogs()) {
            System.out.println(log);
            if (++cnt >= 10) break;
        }
        assertTrue("Backlog count should be non-negative", total >= 0);*/
    }
}
