package com.tangokk.commons.buffer;


import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircularFifoBufferTest {

    static Logger logger = LoggerFactory.getLogger(CircularFifoBufferTest.class);

    @Test
    public void testBuffer() {
        CircularFifoBuffer buffer = new CircularFifoBuffer(10000);
        int producerCount = 10;
        int produceCount = 2000;

        Set<Item> resultSet = new LinkedHashSet<>();
        CountDownLatch consumeLatch = new CountDownLatch(1);
        AtomicInteger produceLatch = new AtomicInteger(producerCount);
        for(int i=0;i<producerCount;i++) {
            new Thread(new Producer(i, produceCount, buffer, produceLatch)).start();
        }


        new Thread(new Consumer(produceLatch, consumeLatch, buffer, items -> {
            System.out.println("add size: " + items.size());
            resultSet.addAll(items);

            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        })).start();
        try {
            consumeLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals( producerCount * produceCount, resultSet.size());
    }








    private static class Producer implements Runnable {

        int produceCount;
        int id;
        CircularFifoBuffer buffer;
        AtomicInteger latch;
        Random random;


        public Producer(int id, int produceCount, CircularFifoBuffer buffer, AtomicInteger latch) {
            this.id = id;
            this.produceCount = produceCount;
            random = new Random();
            this.buffer = buffer;
            this.latch = latch;
        }

        @Override
        public void run() {
            for(int i = 0;i<produceCount;i++) {
                try {
                }catch (Exception e) {
                    logger.error("wtf", e);
                }

                buffer.add(new Item(id, i));
            }

            System.out.println("producer " + id + " finish " + produceCount);
            latch.decrementAndGet();
        }
    }


    static class Consumer implements Runnable {

        private static final int FETCH_ONCE = 1000;
        AtomicInteger produceLatch;
        CountDownLatch latch;
        CircularFifoBuffer buffer;
        DataProcessor processor;

        public Consumer(AtomicInteger produceLatch, CountDownLatch latch, CircularFifoBuffer buffer, DataProcessor processor) {
            this.produceLatch = produceLatch;
            this.latch = latch;
            this.buffer = buffer;
            this.processor = processor;
        }


        @Override
        public void run() {
            while(true) {
                if(produceLatch.get() == 0) {
                    Object [] item = buffer.getAll();
                    processor.process(convert(item));
                    buffer.remove(item.length);
                    break;
                }
                Object [] item = buffer.get(FETCH_ONCE);
                if(item.length == 0) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                processor.process(convert(item));
                buffer.remove(item.length);
                if(item.length < FETCH_ONCE) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            latch.countDown();
        }


        private List<Item> convert(Object [] objects) {
            if(objects == null) {
                return Collections.emptyList();
            }
            return Stream.of(objects)
                .map(o -> (Item) o)
                .collect(Collectors.toList());
        }

        public interface DataProcessor {
            void process(List<Item> items);
        }
    }






    private static class Item {
        int producerId;
        int itemId;


        public Item(int producerId, int itemId) {
            this.producerId = producerId;
            this.itemId = itemId;
        }

        public int getProducerId() {
            return producerId;
        }

        public void setProducerId(int producerId) {
            this.producerId = producerId;
        }

        public int getItemId() {
            return itemId;
        }

        public void setItemId(int itemId) {
            this.itemId = itemId;
        }

        public String getKey() {
            return producerId + "-" +itemId;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj){
                return true;
            }
            if(obj instanceof Item){
                return ((Item) obj).getKey().equals(getKey());
            }

            return false;
        }

        @Override
        public int hashCode() {
            return getKey().hashCode();
        }
    }


}
