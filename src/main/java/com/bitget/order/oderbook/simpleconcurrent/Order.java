package com.bitget.order.oderbook.simpleconcurrent;

public class Order {
    int price;
    int amount;
    int time;
    int type; // 0: 买单, 1: 卖单

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