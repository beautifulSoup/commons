package com.tangokk.commons.buffer;

import com.tangokk.commons.utils.ArrayUtils;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 允许多线程写
 * 只允许单线程->读->处理->移除
 */
public class CircularFifoBuffer {



    private transient Object[] elements;

    private transient int start = 0;
    private transient int end = 0;

    private transient boolean full = false;

    private final int maxElements;

    private ReentrantLock addLock;

    private Semaphore semaphore;

    public CircularFifoBuffer(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0");
        }
        elements = new Object[size];
        maxElements = elements.length;
        addLock = new ReentrantLock();
        semaphore = new Semaphore(size);
    }


    public int size() {
        int size = 0;

        if (end < start) {
            size = maxElements - start + end;
        } else if (end == start) {
            size = (full ? maxElements : 0);
        } else {
            size = end - start;
        }

        return size;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isFull() {
        return size() == maxElements;
    }

    public int maxSize() {
        return maxElements;
    }

    public void clear() {
        full = false;
        start = 0;
        end = 0;
        Arrays.fill(elements, null);
    }

    public boolean add(Object element) {
        if (null == element) {
            throw new NullPointerException("Attempted to add null object to buffer");
        }

        addLock.lock();
        try {
            semaphore.acquire();
        } catch (Exception e) {
            return false;
        }

        elements[end++] = element;


        if (end >= maxElements) {
            end = 0;
        }

        if (end == start) {
            full = true;
        }

        addLock.unlock();

        return true;

    }

    public Object get() {
        if (isEmpty()) {
            return null;
        }

        return elements[start];
    }


    public Object remove() {
        if (isEmpty()) {
            return null;
        }

        Object element = elements[start];
        if(null != element) {
            elements[start++] = null;
            if (start >= maxElements) {
                start = 0;
            }
            full = false;
            semaphore.release();
        }
        return element;
    }


    /**
     * @param size the max size of elements will return
     */
    public Object[] get(int size) {
        int queueSize = size();
        if (queueSize == 0) { //empty
            return new Object[0];
        }
        int realFetchSize =  queueSize >= size ? size : queueSize;
        if (end > start) {
            return Arrays.copyOfRange(elements, start, start + realFetchSize);
        } else {
            if (maxElements - start >= realFetchSize) {
                return Arrays.copyOfRange(elements, start, start + realFetchSize);
            } else {
                return ArrayUtils.mergeTwoArray(
                    Arrays.copyOfRange(elements, start, maxElements),
                    Arrays.copyOfRange(elements, 0, realFetchSize - (maxElements - start))
                );
            }
        }
    }


    public Object[] getAll() {
        return get(size());
    }



    public Object[] remove(int size) {
        if(isEmpty()) {
            return new Object[0];
        }
        int queueSize = size();
        int realFetchSize = queueSize >= size ? size : queueSize;
        Object [] retArr = new Object[realFetchSize];
        for(int i=0;i<realFetchSize;i++) {
            retArr[i] = remove();
        }

        return retArr;
    }





}
