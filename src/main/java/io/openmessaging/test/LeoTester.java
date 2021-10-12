package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.solve.LeoMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LeoTester {

    private final MessageQueue messageQueue = new LeoMessageQueueImpl();

    private final ExecutorService executor = Executors.newFixedThreadPool(40);
    private ConcurrentHashMap<String, HashMap<Integer, HashMap<Integer, ByteBuffer>>> dataMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        new LeoTester().start();
        System.out.println("FINISH " + (System.currentTimeMillis() - start));
    }

    private void start() throws InterruptedException {
        int partySize = 30;
        CyclicBarrier startBarrier = new CyclicBarrier(partySize);
        AtomicInteger atomicInteger = new AtomicInteger();
        for (int t = 0; t < partySize; t++) {
            String tStr = String.valueOf(t);
            executor.execute(() -> {
                try {
                    startBarrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                for (int d = 0; d < 1000; d++) {
                    String text = tStr + "-" + d;
                    ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                    long offset = messageQueue.append("topic" + tStr, 1, buf);
                }
                atomicInteger.getAndIncrement();
            });
        }
        while(atomicInteger.get() != partySize){
            Thread.sleep(1);
        }
    }
}
