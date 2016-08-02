package com.meitu.generate2;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 元素加入队列时，导致大小超过sizeToNotice，通知consumerList中消费者消费
 */
public class ConsumerNoticeConcurrentLinkedQueue<E> extends ConcurrentLinkedDeque<E> {
    private List<Consumer> consumerList;
    private int sizeToNotice;
    private boolean enableNotice;
    // 1.ConcurrentLinkedQueue.size()是扫描一遍获取的，效率低
    // 2.size会有时延。长时间不add/remove，size是正确值
    private AtomicInteger size;

    public ConsumerNoticeConcurrentLinkedQueue() {
        super();
        size = new AtomicInteger(0);
        enableNotice = false;
    }

    public void enableNotice() {
        this.enableNotice = true;
    }

    public void disableNotice() {
        this.enableNotice = false;
    }

    public void setNoticeSize(int sizeToNotice) {
        this.sizeToNotice = sizeToNotice;
    }

    public void setNoticeList(List<Consumer> consumerList) {
        this.consumerList = consumerList;
    }

    private void notice() {
        int cur = size.get();
        if (cur >= sizeToNotice) {
            if (consumerList == null || !enableNotice)
                return;
            for (Consumer t : consumerList) {
                if (t != null)
                    t.consume();
            }
        }
    }

    private void sizeIncr(int num) {
        int cur = size.get();
        while (!size.compareAndSet(cur, cur + num)) {
            cur = size.get();
        }
    }

    private void sizeDecr(int num) {
        int cur = size.get();
        while (!size.compareAndSet(cur, cur - num)) {
            cur = size.get();
        }
    }

    private void sizeClear() {
        int cur = size.get();
        while (size.compareAndSet(cur, 0)) {
            cur = size.get();
        }
    }

    @Override
    public void addFirst(E e) {
        super.addFirst(e);
        sizeIncr(1);
        notice();
    }

    @Override
    public void addLast(E e) {
        super.addLast(e);
        sizeIncr(1);
        notice();
    }

    @Override
    public boolean add(E e) {
        boolean result = super.add(e);
        if (result) {
            sizeIncr(1);
            notice();
        }
        return result;
    }

    /**
     * ConcurrentLinkedQueue注释：@return {@code true} if this deque changed as a
     * result of the call
     */
    @Override
    @Deprecated
    public boolean addAll(Collection<? extends E> c) {
        return false;
    }

    @Override
    public E removeFirst() {
        E e = super.removeFirst();
        sizeDecr(1);// remove失败上一行会抛异常
        return e;
    }

    @Override
    public E removeLast() {
        E e = super.removeLast();
        sizeDecr(1);
        return e;
    }

    @Override
    public E remove() {
        E e = super.remove();
        sizeDecr(1);
        return e;
    }

    @Override
    @Deprecated
    public boolean removeFirstOccurrence(Object o) {
        return false;
    }

    @Override
    @Deprecated
    public boolean removeLastOccurrence(Object o) {
        return false;
    }

    @Override
    @Deprecated
    public boolean remove(Object o) {
        return false;
    }

    @Override
    @Deprecated
    public boolean offerFirst(E e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean offerLast(E e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public E peekFirst() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E peekLast() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E getFirst() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E getLast() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E pollFirst() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean offer(E e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public E poll() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E peek() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public E element() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public void push(E e) {
        // TODO Auto-generated method stub
        return;
    }

    @Override
    @Deprecated
    public E pop() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return this.size.get();
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
        super.clear();
        this.sizeClear();
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return super.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        // TODO Auto-generated method stub
        return super.toArray(a);
    }

    @Override
    public Iterator<E> iterator() {
        // TODO Auto-generated method stub
        return super.iterator();
    }

    @Override
    public Iterator<E> descendingIterator() {
        // TODO Auto-generated method stub
        return super.descendingIterator();
    }

    @Override
    public E pollLast() {
        E e = super.pollLast();
        if (e != null)
            this.sizeDecr(1);
        return e;
    }
}