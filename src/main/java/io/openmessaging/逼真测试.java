package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import io.openmessaging.solve.LeoMessageQueueImpl;

public class 逼真测试 {

    private static final int THREAD_COUNT = 10;
    private static final int TOPIC_COUNT = 100;
    private static final int MSG_COUNT_MIN = 1;
    private static final int MSG_COUNT_MAX = 30;
    private static final int DATA_SIZE_MIN = 100;
    private static final int DATA_SIZE_MAX = 17 * 1024;
    private static final long RANDOM_SEED = 157625465123L;
    private static final long MAX_SIZE_PER_THREAD = 1 * 1024 * 1024 * 1024L; // 1G
//    private static final int QUEUE_COUNT_MIN = 1;
//    private static final int QUEUE_COUNT_MAX = 10000;

    private static Random random = new Random(RANDOM_SEED);

    private static class Message {
        public String topic;
        public int queueId;
        public int dataSize;
        public int offset;

        public Message(String topic, int queueId, int dataSize, int offset) {
            this.topic = topic;
            this.queueId = queueId;
            this.dataSize = dataSize;
            this.offset = offset;
        }
    }

    private static byte[] bytes = new byte[17 * 1024];

    public static void main(String[] args) throws InterruptedException {
        random.nextBytes(bytes);
        long totalSize = 0L;
        long[] threadMsgSize = new long[THREAD_COUNT];
        Thread[] threads = new Thread[THREAD_COUNT];
        List<Message>[] msgListArray = new List[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            msgListArray[i] = new ArrayList<>();
        }
        for (int i = 0; i < TOPIC_COUNT; i++) {
//            int queueCount = QUEUE_COUNT_MIN + random.nextInt(QUEUE_COUNT_MAX);
            int queueId = 0;
            while (threadMsgSize[i % THREAD_COUNT] < MAX_SIZE_PER_THREAD) {
                int msgCount = MSG_COUNT_MIN + random.nextInt(MSG_COUNT_MAX);
                for (int msgIndex = 0; msgIndex < msgCount; msgIndex++) {
                    int dataSize = DATA_SIZE_MIN + random.nextInt(DATA_SIZE_MAX - DATA_SIZE_MIN + 1);
                    Message msg = new Message("topic" + i, queueId, dataSize, msgIndex);
                    msgListArray[i % THREAD_COUNT].add(msg);
                    threadMsgSize[i % THREAD_COUNT] += dataSize;
                    totalSize += dataSize;
                }
                queueId++;
            }
//            for (int queueId = 0; queueId < queueCount; queueId++) {
//                int msgCount = MSG_COUNT_MIN + random.nextInt(MSG_COUNT_MAX);
//                for (int msgIndex = 0; msgIndex < msgCount; msgIndex++) {
//                    int dataSize = DATA_SIZE_MIN + random.nextInt(DATA_SIZE_MAX - DATA_SIZE_MIN + 1);
//                    Message msg = new Message("topic" + i, queueId, dataSize);
//                    msgListArray[i % THREAD_COUNT].add(msg);
//                    threadMsgSize[i % THREAD_COUNT] += dataSize;
//                    totalSize += dataSize;
//                }
//            }
        }

        for (int i = 0; i < threadMsgSize.length; i++) {
            System.out.println("thread-" + i + " size : " + (threadMsgSize[i] / 1024.f / 1024.f) + " MB");
        }
        System.out.println("totalSize : " + totalSize / 1024.f / 1024.f / 1024.f + " GB");

        MessageQueue messageQueue = new LeoMessageQueueImpl();

        for (int i = 0; i < threads.length; i++) {
            int finalI = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    List<Message> messages = msgListArray[finalI];
                    for (Message message : messages) {
                        ByteBuffer buf = ByteBuffer.allocate(message.dataSize);
                        buf.putInt(message.offset);
                        buf.put(bytes, 0, message.dataSize - 4);
                        messageQueue.append(message.topic, message.queueId, buf);
                    }
                    System.out.println("THREAD-" + finalI + " 完成");
                }
            });
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - startTime));


//        long start = System.currentTimeMillis();
//        Map<Integer, ByteBuffer> range;
//        for (; i < 10000; i++) {
//            String text = String.valueOf(i);
//            ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
//            messageQueue.append("A", 1, buf);
//        }
//        Thread threadW1 = new Thread(() -> {
//            for (; i < 20000; i++) {
//                String text = String.valueOf(i);
//                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
//                messageQueue.append("A", 1, buf);
//            }
//        });
//        Thread threadR1 = new Thread(() -> {
//            System.out.println("边写边查 threadR1");
//            messageQueue.getRange("A", 1, 9950, 100)
//                    .forEach((key, value) -> System.out.println(key + ": " + new String(value.array())));
//        });
////        Thread threadW2 = new Thread(() -> {
////            for (; i < 30000; i++) {
////                String text = String.valueOf(i);
////                ByteBuffer buf = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
////                messageQueue.append("B", 1, buf);
////            }
////        });
//        threadW1.start();
//        threadW1.join();
//        threadR1.start();
//        threadR1.join();
////        threadW2.start();
////        threadW2.join();
////        System.out.println("最后查");
////        messageQueue.getRange("A", 1, 9950, 100)
////                .forEach((key, value) -> System.out.println("最后查: " + key + ": " + new String(value.array())));
//        System.out.println(System.currentTimeMillis() - start);
    }
}
