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

    private final Object WRITE_LOCKER = new Object();
    private final Semaphore semaphore = new Semaphore(20);
    private final int barrierCount = THREAD_MAX / 2;
    private final CyclicBarrier barrier = new CyclicBarrier(barrierCount);

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data, Indexer indexer) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        int position;
        try {
            semaphore.acquire();
            synchronized (WRITE_LOCKER) {
                if (logMappedBuf.remaining() < dataSize) {
                    logMappedBuf.force();
                    unmap(logMappedBuf);
                    logFileChannel.close();
                    openLog();
                }
                position = logMappedBuf.position();
                logMappedBuf.put(topic); // 1
                logMappedBuf.putShort(queueId); // 2
                logMappedBuf.putInt(offset); // 4
                logMappedBuf.putShort(msgLen); // 2
                logMappedBuf.put(data);
            }
            try {
                int arrive = barrier.await(250, TimeUnit.MILLISECONDS);
                if (arrive == 0) {
                    System.out.println("SNE-F");
                    synchronized (WRITE_LOCKER) {
                        logMappedBuf.force();
                    }
                    semaphore.release(20);
                }
            } catch (TimeoutException e) {
                // 只有一个超时，其他都是 BrokenBarrierException
                System.out.println("TO-F");
                synchronized (WRITE_LOCKER) {
                    barrier.reset();
                    logMappedBuf.force();
                }
                semaphore.release();
            } catch (BrokenBarrierException | InterruptedException e) {
                semaphore.release();
            }
            indexer.writeIndex(id, logNumAdder, position, dataSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static class ForceTest extends Thread{
//        @Override
//        public void run() {
//            try {
//                int arrive = barrier1.await(1000, TimeUnit.MILLISECONDS);
//                if (arrive == 0) {
//                    System.out.println(arrive + "SNE-F");
//                }
//            } catch (TimeoutException e) {
//                barrier1.reset();
//                System.out.println("TO-F");
//            } catch (BrokenBarrierException e) {
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//   static CyclicBarrier barrier1 = new CyclicBarrier(20);
//
//    public static void main(String[] args) throws InterruptedException {
//        ForceTest[] forceTests = new ForceTest[19];
//        for (int i = 0; i < 19; i++) {
//            forceTests[i] = new ForceTest();
//            forceTests[i].start();
//        }
//        for (int i = 0; i < 19; i++) {
//            forceTests[i].join();
//        }
//        System.out.println("all done");
//        Thread.sleep(10_000);
//        for (int i = 0; i < 19; i++) {
//            forceTests[i] = new ForceTest();
//            forceTests[i].start();
//        }
//        for (int i = 0; i < 19; i++) {
//            forceTests[i].join();
//        }
//        System.out.println("all done");
//        Thread.sleep(10_000);
//    }

}
