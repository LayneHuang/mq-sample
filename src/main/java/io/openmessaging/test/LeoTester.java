package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.solve.LayneBMessageQueueImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class LeoTester {

    private final MessageQueue messageQueue = new LayneBMessageQueueImpl();

    private final ExecutorService executor = Executors.newFixedThreadPool(40);
    private ConcurrentHashMap<String, HashMap<Integer, HashMap<Integer, ByteBuffer>>> dataMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        new LeoTester().start();
        System.out.println("FINISH " + (System.currentTimeMillis() - start));
        System.exit(0);
    }

    private void start() throws InterruptedException {
        int partySize = 40;
        CyclicBarrier startBarrier = new CyclicBarrier(partySize);
        CountDownLatch latch = new CountDownLatch(partySize);
        for (int t = 0; t < partySize; t++) {
            String tStr = String.valueOf(t);
            executor.execute(() -> {
                try {
                    startBarrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                for (int d = 0; d < 1000; d++) {
                    String text = tStr + "-" + d + getRandomString();
                    ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
                    long offset = messageQueue.append("topic" + tStr, 1, buf);
                }
                latch.countDown();
            });
        }
        latch.await();
        messageQueue.getRange("topic29", 1, 995, 10)
                .forEach((key, value) -> System.out.println("边写边查 topic29: " + key + ": " + new String(value.array())));
    }

    Random random = new Random();

    private String getRandomString() {
        return random.ints(random.nextInt(1), 'A', 'z')
                .mapToObj(letter -> {
                    char letter1 = (char) letter;
                    return String.valueOf(letter1);
                })
                .collect(Collectors.joining());
    }

}
