package com.bitget.order.oderbook.simpleconcurrent;

import com.github.rohansuri.art.AdaptiveRadixTree;
import com.github.rohansuri.art.BinaryComparables;

import java.util.Map;
import java.util.NavigableMap;


@Deprecated
public class OrderBookManagement {

    private static final int MOD = 1_000_000_007;
    // 为了转换买单价格，我们假设订单价格不会超过 10^9
    private static final int PRICE_BASE = 1_000_000_000;

    // 将卖单价格转换为固定长度字符串，保证自然升序排序
    private static String sellKey(int price) {
        return String.format("%010d", price);
    }

    // 将买单价格转换为固定长度字符串，为保证买单降序，
    // 转换方式是 PRICE_BASE - price，较小的转换结果对应较高的实际价格
    private static String buyKey(int price) {
        return String.format("%010d", PRICE_BASE - price);
    }

    public static int backLogManagement(int[][] orders) {
        // 使用 ART 实现的 NavigableMap，key 为经过转换的固定宽度字符串，value 为对应价格的订单总数
        NavigableMap<String, Long> buyTree = new AdaptiveRadixTree<>(BinaryComparables.forString());
        NavigableMap<String, Long> sellTree = new AdaptiveRadixTree<>(BinaryComparables.forString());

        for (int[] order : orders) {
            int price = order[0], amount = order[1], orderType = order[2];
            if (orderType == 0) { // 买单
                // 尝试匹配卖单：卖单的 key 为 sellKey(price)，其自然升序排列
                while (amount > 0 && !sellTree.isEmpty()) {
                    // 取出卖单中最低价格的订单
                    Map.Entry<String, Long> entry = sellTree.firstEntry();
                    int sellPrice = Integer.parseInt(entry.getKey());
                    // 如果最低卖单价格大于买单价格，则无法匹配
                    if (sellPrice > price) break;
                    long available = entry.getValue();
                    int match = Math.min(amount, (int) available);
                    amount -= match;
                    if (available > match) {
                        // 部分匹配，更新卖单剩余数量
                        sellTree.put(entry.getKey(), available - match);
                    } else {
                        // 完全匹配，删除该卖单
                        sellTree.pollFirstEntry();
                    }
                }
                // 如果买单还有剩余，加入买单树
                if (amount > 0) {
                    String key = buyKey(price);
                    buyTree.put(key, buyTree.getOrDefault(key, 0L) + amount);
                }
            } else { // 卖单
                // 尝试匹配买单：买单的 key 为 buyKey(price)（转换后较小的代表较高的实际价格）
                while (amount > 0 && !buyTree.isEmpty()) {
                    Map.Entry<String, Long> entry = buyTree.firstEntry();
                    // 计算实际买单价格：buyKey = String.format("%010d", PRICE_BASE - price)
                    int buyTransformed = Integer.parseInt(entry.getKey());
                    int actualBuyPrice = PRICE_BASE - buyTransformed;
                    if (actualBuyPrice < price) break;
                    long available = entry.getValue();
                    int match = Math.min(amount, (int) available);
                    amount -= match;
                    if (available > match) {
                        buyTree.put(entry.getKey(), available - match);
                    } else {
                        buyTree.pollFirstEntry();
                    }
                }
                if (amount > 0) {
                    String key = sellKey(price);
                    sellTree.put(key, sellTree.getOrDefault(key, 0L) + amount);
                }
            }
        }

        // 统计所有剩余订单数量
        long total = 0;
        for (long cnt : buyTree.values()) {
            total = (total + cnt) % MOD;
        }
        for (long cnt : sellTree.values()) {
            total = (total + cnt) % MOD;
        }
        return (int) total;
    }

    // 简单测试
    public static void main(String[] args) {
        // 示例输入，每个订单格式为 [price, amount, orderType], 0 为 买单，1为卖单
        int[][] orders = {
                {10, 5, 0},
                {15, 2, 1},
                {25, 1, 1},
                {30, 4, 0},
                {20, 3, 1}
        };
        int result = backLogManagement(orders);
        System.out.println("Backlog orders count: " + result);
    }

}
