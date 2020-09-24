package com.tangokk.commons.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 允许多线程写
 * 允许多线程->读->处理->移除，单个消费者只能消费一个segment
 */
public class SegmentsCircularFifoBuffer {

    private Logger logger = LoggerFactory.getLogger(SegmentsCircularFifoBuffer.class.getName());


    int segmentCount;

    CircularFifoBuffer [] segmentBuffers;

    public SegmentsCircularFifoBuffer(int size, int segmentCount) {
        if (size <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0");
        }
        this.segmentCount = segmentCount;
        segmentBuffers = buildSegments(size, segmentCount);
    }



    private CircularFifoBuffer [] buildSegments(int size, int segmentCount) {
        int segmentSize = (int)Math.ceil(size / segmentCount);
        CircularFifoBuffer [] segments = new CircularFifoBuffer[segmentCount];
        for(int i = 0;i<segmentCount;i++) {
            segments[i] = new CircularFifoBuffer(segmentSize);
        }
        return segments;
    }


    public int size(int segmentId) {
        return segmentBuffers[segmentId].size();
    }

    public boolean isEmpty(int segmentId) {
        return segmentBuffers[segmentId].isEmpty();
    }

    public boolean isFull(int segmentId) {
        return segmentBuffers[segmentId].isFull();
    }

    public int maxSize(int segmentId) {
        return segmentBuffers[segmentId].maxSize();
    }

    public void clear(int segmentId) {
        segmentBuffers[segmentId].clear();
    }

    public boolean add(Object element) {
        if (null == element) {
            throw new NullPointerException("Attempted to add null object to buffer");
        }
        int segmentId = element.hashCode() % segmentCount;
        segmentBuffers[segmentId].add(element);
        return true;

    }

    /**
     * @param size the max size of elements will return
     */
    public Object[] get(int segmentId, int size) {
        return segmentBuffers[segmentId].get(size);
    }


    public Object[] getAll(int segmentId) {
        return segmentBuffers[segmentId].getAll();
    }



    public Object[] remove(int segmentId, int size) {
        return segmentBuffers[segmentId].remove(size);
    }





}
