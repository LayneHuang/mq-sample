package io.openmessaging.leo2;

import io.openmessaging.leo.Indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;

import static io.openmessaging.leo2.DataManager.*;

public class DataBlock {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;

    public DataBlock(byte id) {
        this.id = id;
        logDir = LOGS_PATH.resolve(String.valueOf(this.id));
        try {
            Files.createDirectories(logDir);
            setupLog();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openLog() throws IOException {
        logNumAdder++;
        setupLog();
    }

    private void setupLog() throws IOException {
        Path logFile = logDir.resolve(String.valueOf(logNumAdder));
        Files.createFile(logFile);
        logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);// 1G
    }

    public static final int barrierCount = THREAD_MAX / 2;
    private final Object WRITE_LOCKER = new Object();
    private final Semaphore semaphore = new Semaphore(barrierCount, true);
    private final CyclicBarrier barrier = new CyclicBarrier(barrierCount);

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data, Indexer indexer) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        try {
            semaphore.acquire();
            synchronized (WRITE_LOCKER) {
                if (logMappedBuf.remaining() < dataSize) {
                    logMappedBuf.force();
                    unmap(logMappedBuf);
                    logFileChannel.close();
                    openLog();
                }
                int position = logMappedBuf.position();
                logMappedBuf.put(topic); // 1
                logMappedBuf.putShort(queueId); // 2
                logMappedBuf.putInt(offset); // 4
                logMappedBuf.putShort(msgLen); // 2
                logMappedBuf.put(data);
                indexer.writeIndex(id, logNumAdder, position, dataSize);
            }
            MappedByteBuffer tempBuf = logMappedBuf;
            try {
                int arrive = barrier.await(250, TimeUnit.MILLISECONDS);
                if (arrive == 0) {
                    System.out.println("Full-F");
                    synchronized (WRITE_LOCKER) {
                        tempBuf.force();
                    }
                }
            } catch (TimeoutException e) {
                // 只有一个超时，其他都是 BrokenBarrierException
                System.out.println("Timeout-F");
                synchronized (WRITE_LOCKER) {
                    tempBuf.force();
                    barrier.reset();
                }
            } catch (BrokenBarrierException | InterruptedException e) {
                System.out.println("Broken");
            }
            semaphore.release();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    static final CyclicBarrier barrier1 = new CyclicBarrier(barrierCount, () -> {
//        System.out.println("SNE-F");
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("sleep end");
//    });
//
//    public static class ForceTest extends Thread {
//        @Override
//        public void run() {
//            try {
//                 barrier1.await(2000, TimeUnit.MILLISECONDS);
//            } catch (TimeoutException e) {
//                barrier1.reset();
//                System.out.println("TimeoutException");
//            } catch (BrokenBarrierException e) {
//                System.out.println("BrokenBarrierException");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        int testCount = 20;
//        ForceTest[] forceTests = new ForceTest[testCount];
//        for (int i = 0; i < testCount; i++) {
//            forceTests[i] = new ForceTest();
//            forceTests[i].start();
//        }
//        for (int i = 0; i < 19; i++) {
//            forceTests[i].join();
//        }
//        System.out.println("all done");
//        Thread.sleep(10_000);
//        for (int i = 0; i < 5; i++) {
//            forceTests[i] = new ForceTest();
//            forceTests[i].start();
//        }
//        for (int i = 0; i < 5; i++) {
//            forceTests[i].join();
//        }
//        System.out.println("all done");
//        Thread.sleep(10_000);
//    }

}
