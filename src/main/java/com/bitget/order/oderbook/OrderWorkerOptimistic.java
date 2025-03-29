package com.bitget.order.oderbook;

import com.lmax.disruptor.WorkHandler;

public class OrderWorkerOptimistic implements WorkHandler<OrderEvent> {
    private final ConcurrentOrderBookOptimistic orderBook;
    public OrderWorkerOptimistic(ConcurrentOrderBookOptimistic orderBook) {
        this.orderBook = orderBook;
    }
    @Override
    public void onEvent(OrderEvent event) {
        // 对单个订单进行处理
        orderBook.process(event.order);
    }
}