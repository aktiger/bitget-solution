package com.bitget.order.oderbook;

import com.lmax.disruptor.WorkHandler;

public class OrderWorker implements WorkHandler<OrderEvent> {
    private final ConcurrentOrderBook orderBook;
    public OrderWorker(ConcurrentOrderBook orderBook) {
        this.orderBook = orderBook;
    }
    @Override
    public void onEvent(OrderEvent event) {
        // 对单个订单进行处理
        orderBook.process(event.order);
    }
}