package com.bitget.order.oderbook.simpleconcurrent;

import com.lmax.disruptor.EventFactory;

// EventFactory 用于创建 OrderEvent 实例
public class OrderEventFactory implements EventFactory<OrderEvent> {
    @Override
    public OrderEvent newInstance() {
        return new OrderEvent();
    }
}
