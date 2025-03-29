# Order Book Implementation using Adaptive Radix Tree (ART)

This project implements an order book using the Adaptive Radix Tree (ART) data structure. The order book is designed to efficiently handle large volumes of orders, providing high-performance insertion, search, and deletion operations.

## Features

- **Price Priority**: Orders are prioritized based on price, with higher buy orders and lower sell orders being matched first.
- **Efficient Insertion**: Supports efficient insertion of new orders into the order book.
- **Efficient Deletion**: Supports efficient deletion of existing orders from the order book.
- **Order Matching**: Efficiently matches buy and sell orders based on price and quantity.
- **High-Performance Data Structure**: Utilizes the Adaptive Radix Tree (ART) for high-performance order management.

## Installation

To use this project, you need to have Java installed on your system. You can clone the repository and build the project using Maven.

```sh
# Clone the repository
git clone https://github.com/aktiger/bitget-solution

# Navigate to the project directory
cd bitget-solution

# Build the project using Maven
mvn clean install
```

## Usage
Here is an example of how to use the order book implementation in your Java application:

```
package com.bitget.order.oderbook.optimistic.nodisrupter;

import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

// ----------------------
// 5. 测试入口：使用Junit多线程测试
// 这里不再使用Disruptor，而是使用一个简单的BlockingQueue来发布订单，从而减少Disruptor调度开销
class BacklogOrdersWithTime {
    public static final int PRICE_BASE = 1_000_000_000;
}

public class BacklogOrdersConcurrentOptimisticPartitionedTest2 {

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

        // 使用128个分区
        int partitions = 128;
        PartitionedConcurrentOrderBookOptimistic2 partitionedBook =
                new PartitionedConcurrentOrderBookOptimistic2(partitions);

        // 使用 BlockingQueue 发布订单，避免Disruptor额外开销
        BlockingQueue<Order> orderQueue = new ArrayBlockingQueue<>(orderCount);
        for (int i = 0; i < orderCount; i++) {
            Order order = new Order(orders[i][0], orders[i][1], i, orders[i][2]);
            orderQueue.offer(order);
        }

        int numWorkers = Runtime.getRuntime().availableProcessors();
        ExecutorService workerPool = Executors.newFixedThreadPool(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            workerPool.submit(new OrderWorkerOptimisticPartitioned2(partitionedBook, orderQueue));
        }
        long startTime = System.nanoTime();
        workerPool.shutdown();
        workerPool.awaitTermination(1, TimeUnit.MINUTES);
        long endTime = System.nanoTime();
        long elapsedMs = (endTime - startTime) / 1_000_000;

        long total = partitionedBook.getBacklogCount();
        System.out.println("Processed " + orderCount + " orders concurrently (partitioned) in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + total);
        System.out.println("Sample match log entries (first 10):");
        /*List<String> logs = partitionedBook.getMatchLogs();
        for (int i = 0; i < Math.min(10, logs.size()); i++) {
            System.out.println(logs.get(i));
        }
        assertTrue("Backlog count should be non-negative", total >= 0);*/
    }
}
```




