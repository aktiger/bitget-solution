package com.bitget.order.oderbook;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.*;


public class BacklogOrdersWithTime {



    // 模数
    private static final int MOD = 1_000_000_007;
    // 假设订单价格不会超过 10^9
    private static final int PRICE_BASE = 1_000_000_000;

    // 将卖单价格转换为固定宽度字符串（升序）
    private static String sellKey(int price) {
        return String.format("%010d", price);
    }

    // 将买单价格转换为固定宽度字符串，利用转换使得较高的实际价格对应较小的字符串（降序）
    private static String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    // 定义订单类，time 表示下单时间，数值越小越早
    static class Order {
        int price;
        int amount;
        int time;
        int type; // 0:买单, 1:卖单

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

    // 用于存放ART匹配过程的结果
    static class OrderProcessResult {
        int backlog;
        List<String> matchLog;
        NavigableMap<String, LinkedList<Order>> buyTree;
        NavigableMap<String, LinkedList<Order>> sellTree;
    }

    /**
     * 处理订单匹配，订单数组每行格式为 [price, amount, orderType]，
     * 下标即为下单时间（越小越早）。
     * 返回包含匹配过程日志和最终买单、卖单状态的结果。
     */
    public static OrderProcessResult processOrders(int[][] orders) {
        // 使用 ART 实现的 NavigableMap，值类型为 FIFO 队列保存同一价格的多个订单
        NavigableMap<String, LinkedList<Order>> buyTree =
                new AdaptiveRadixTree<>(BinaryComparables.forString());
        NavigableMap<String, LinkedList<Order>> sellTree =
                new AdaptiveRadixTree<>(BinaryComparables.forString());

        List<String> matchLog = new ArrayList<>();

        // 按订单输入顺序处理，下标作为时间戳
        for (int i = 0; i < orders.length; i++) {
            int price = orders[i][0], amount = orders[i][1], type = orders[i][2];
            Order currentOrder = new Order(price, amount, i, type);
            if (type == 0) { // 买单
                // 尝试匹配卖单（卖单价格最低优先，且卖单价格 <= 买单价格）
                while (currentOrder.amount > 0 && !sellTree.isEmpty()) {
                    Map.Entry<String, LinkedList<Order>> entry = sellTree.firstEntry();
                    int sellPrice = Integer.parseInt(entry.getKey());
                    if (sellPrice > currentOrder.price) break;
                    LinkedList<Order> sellQueue = entry.getValue();
                    // FIFO：取队头最早的卖单
                    Order sellOrder = sellQueue.peek();
                    int matchQty = Math.min(currentOrder.amount, sellOrder.amount);
                    // 记录匹配过程
                    matchLog.add(String.format("Match: %s matched with %s, quantity=%d",
                            currentOrder, sellOrder, matchQty));
                    // 更新双方剩余量
                    currentOrder.amount -= matchQty;
                    sellOrder.amount -= matchQty;
                    if (sellOrder.amount == 0) {
                        sellQueue.poll(); // 移除已完全成交的卖单
                        if (sellQueue.isEmpty()) {
                            sellTree.pollFirstEntry();
                        }
                    }
                }
                // 如果买单未完全成交，将剩余订单加入买单树
                if (currentOrder.amount > 0) {
                    String key = buyKey(currentOrder.price);
                    LinkedList<Order> list = buyTree.getOrDefault(key, new LinkedList<>());
                    list.add(currentOrder);
                    buyTree.put(key, list);
                    matchLog.add(String.format("Buy order added to backlog: %s", currentOrder));
                }
            } else { // 卖单
                // 尝试匹配买单（买单价格最高优先，即买树中最小的 key对应最高买价，且实际买价 >= 卖单价格）
                while (currentOrder.amount > 0 && !buyTree.isEmpty()) {
                    Map.Entry<String, LinkedList<Order>> entry = buyTree.firstEntry();
                    int transformed = Integer.parseInt(entry.getKey());
                    int actualBuyPrice = PRICE_BASE - transformed;
                    if (actualBuyPrice < currentOrder.price) break;
                    LinkedList<Order> buyQueue = entry.getValue();
                    Order buyOrder = buyQueue.peek();
                    int matchQty = Math.min(currentOrder.amount, buyOrder.amount);
                    matchLog.add(String.format("Match: %s matched with %s, quantity=%d",
                            buyOrder, currentOrder, matchQty));
                    currentOrder.amount -= matchQty;
                    buyOrder.amount -= matchQty;
                    if (buyOrder.amount == 0) {
                        buyQueue.poll();
                        if (buyQueue.isEmpty()) {
                            buyTree.pollFirstEntry();
                        }
                    }
                }
                if (currentOrder.amount > 0) {
                    String key = sellKey(currentOrder.price);
                    LinkedList<Order> list = sellTree.getOrDefault(key, new LinkedList<>());
                    list.add(currentOrder);
                    sellTree.put(key, list);
                    matchLog.add(String.format("Sell order added to backlog: %s", currentOrder));
                }
            }
        }

        // 统计剩余订单数量
        long total = 0;
        for (LinkedList<Order> list : buyTree.values()) {
            for (Order o : list) {
                total = (total + o.amount) % MOD;
            }
        }
        for (LinkedList<Order> list : sellTree.values()) {
            for (Order o : list) {
                total = (total + o.amount) % MOD;
            }
        }
        OrderProcessResult res = new OrderProcessResult();
        res.backlog = (int) total;
        res.matchLog = matchLog;
        res.buyTree = buyTree;
        res.sellTree = sellTree;
        return res;
    }

    // 辅助方法，将树中所有订单按价格顺序打印出来
    private static String treeToString(NavigableMap<String, LinkedList<Order>> tree, boolean isBuy) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, LinkedList<Order>> entry : tree.entrySet()) {
            int price;
            if (isBuy) {
                int inv = Integer.parseInt(entry.getKey());
                price = PRICE_BASE - inv;
            } else {
                price = Integer.parseInt(entry.getKey());
            }
            sb.append(String.format("Price: %d -> %s\n", price, entry.getValue()));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        // 示例订单：每个订单格式 [price, amount, orderType]
        // 下标越小表示下单时间越早
        int[][] orders = {
                {10, 5, 0},   // Buy order at price 10, quantity 5, time 0
                {10, 3, 0},   // Buy order at price 10, quantity 3, time 1
                {15, 2, 1},   // Sell order at price 15, quantity 2, time 2
                {10, 4, 1},   // Sell order at price 10, quantity 4, time 3
                {10, 2, 0},   // Buy order at price 10, quantity 2, time 4
                {10, 5, 1}    // Sell order at price 10, quantity 5, time 5
        };

        OrderProcessResult result = processOrders(orders);

        System.out.println("=== Match Log ===");
        for (String log : result.matchLog) {
            System.out.println(log);
        }

        System.out.println("\n=== Final Backlog Orders ===");
        System.out.println("Buy Orders:");
        System.out.println(treeToString(result.buyTree, true));
        System.out.println("Sell Orders:");
        System.out.println(treeToString(result.sellTree, false));

        System.out.println("Total backlog orders count: " + result.backlog);
    }
}
