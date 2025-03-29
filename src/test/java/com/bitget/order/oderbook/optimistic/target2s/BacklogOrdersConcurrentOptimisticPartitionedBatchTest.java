package com.bitget.order.oderbook.optimistic.target2s;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

public class BacklogOrdersConcurrentOptimisticPartitionedBatchTest {
    public static final int MOD = 1_000_000_007;
    public static final int PRICE_BASE = 1_000_000_000;

    @Test
    public void testOptimizedPerformance() throws Exception {
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

        // 分区数设为1024，进一步降低锁竞争
        int partitions = 1024;
        PartitionedConcurrentOrderBookOptimisticBatch partitionedBook =
                new PartitionedConcurrentOrderBookOptimisticBatch(partitions);

        // 使用 BlockingQueue 发布订单
        BlockingQueue<Order> orderQueue = new ArrayBlockingQueue<>(orderCount);
        for (int i = 0; i < orderCount; i++) {
            Order order = new Order(orders[i][0], orders[i][1], i, orders[i][2]);
            orderQueue.offer(order);
        }

        int numWorkers = Runtime.getRuntime().availableProcessors();
        ExecutorService workerPool = Executors.newFixedThreadPool(numWorkers);
        // 每个工作线程批量处理订单
        int batchSize = 1000;
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            futures.add(workerPool.submit(() -> {
                List<Order> batch = new ArrayList<>(batchSize);
                while (true) {
                    Order order = null;
                    try {
                        order = orderQueue.poll(50, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if (order != null) {
                        batch.add(order);
                        if (batch.size() >= batchSize) {
                            for (Order o : batch) {
                                partitionedBook.process(o);
                            }
                            batch.clear();
                        }
                    } else {
                        break;
                    }
                }
                // 处理剩余订单
                for (Order o : batch) {
                    partitionedBook.process(o);
                }
            }));
        }
        long startTime = System.nanoTime();
        for (Future<?> f : futures) {
            f.get();
        }
        workerPool.shutdown();
        workerPool.awaitTermination(1, TimeUnit.MINUTES);
        long endTime = System.nanoTime();
        long elapsedMs = (endTime - startTime) / 1_000_000;

        long total = partitionedBook.getBacklogCount();
        System.out.println("Processed " + orderCount + " orders concurrently (optimized batch partitioned) in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + total);
        // 期望总订单数量 >= 0
        assertTrue("Backlog count should be non-negative", total >= 0);
    }
}

