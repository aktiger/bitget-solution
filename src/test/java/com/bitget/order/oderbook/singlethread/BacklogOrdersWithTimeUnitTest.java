
package com.bitget.order.oderbook.singlethread;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Random;

public class BacklogOrdersWithTimeUnitTest {

    @Test
    public void testPerformance() {
        // 设置订单数量为 1,000,000
        int orderCount = 1_000_000;
        int[][] orders = new int[orderCount][3];
        Random rand = new Random(42);
        // 生成随机订单数据：
        // 价格范围 1 ~ 100,000
        // 数量 1 ~ 10
        // 订单类型：0 表示买单，1 表示卖单
        for (int i = 0; i < orderCount; i++) {
            int price = rand.nextInt(100000) + 1;
            int amount = rand.nextInt(10) + 1;
            int type = rand.nextBoolean() ? 0 : 1;
            orders[i][0] = price;
            orders[i][1] = amount;
            orders[i][2] = type;
        }

        // 记录开始时间
        long start = System.nanoTime();
        // 调用订单匹配处理方法
        BacklogOrdersWithTime.OrderProcessResult result = BacklogOrdersWithTime.processOrders(orders);
        // 记录结束时间
        long end = System.nanoTime();
        long elapsedMs = (end - start) / 1_000_000;

        // 输出性能测试结果及最终剩余订单数量
        System.out.println("Processed " + orderCount + " orders in " + elapsedMs + " ms");
        System.out.println("Total backlog orders count: " + result.backlog);

        assertTrue("Backlog count should be non-negative", result.backlog >= 0);
    }
}
