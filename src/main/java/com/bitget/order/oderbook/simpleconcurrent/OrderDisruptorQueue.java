package com.bitget.order.oderbook.simpleconcurrent;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class OrderDisruptorQueue {
    private static final int BUFFER_SIZE = 4096;
    private final RingBuffer<OrderEvent> ringBuffer;
    // 消费者当前消费到的序列，初始为 -1
    protected final  AtomicLong consumerSequence = new AtomicLong(-1);

    public OrderDisruptorQueue() {
        Disruptor<OrderEvent> disruptor;
        disruptor = new Disruptor<>(
                new OrderEventFactory(),
                BUFFER_SIZE,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new com.lmax.disruptor.YieldingWaitStrategy()
        );
        // 我们不设置实际的 EventHandler，因为队列操作由直接访问 RingBuffer 完成
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    // 将订单添加到队列中
    public void offer(Order order) {
        long seq = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(seq);
            event.order = order;
        } finally {
            ringBuffer.publish(seq);
        }
    }

    // 获取队首元素但不删除
    public Order peek() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        OrderEvent event = ringBuffer.get(nextSeq);
        return event.order;
    }

    // 移除并返回队首元素
    public Order poll() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        OrderEvent event = ringBuffer.get(nextSeq);
        consumerSequence.incrementAndGet();
        return event.order;
    }

    // 检查队列是否为空
    public boolean isEmpty() {
        return (consumerSequence.get() + 1) > ringBuffer.getCursor();
    }

    // 返回队列中的所有订单（仅用于调试）
    public List<Order> toList() {
        List<Order> list = new ArrayList<>();
        long start = consumerSequence.get() + 1;
        long end = ringBuffer.getCursor();
        for (long seq = start; seq <= end; seq++) {
            OrderEvent event = ringBuffer.get(seq);
            list.add(event.order);
        }
        return list;
    }

    @Override
    public String toString() {
        return toList().toString();
    }
}