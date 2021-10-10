package io.openmessaging.leo2;

import io.openmessaging.leo.Indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.openmessaging.leo2.DataManager.*;

public class DataBlock {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;
    private final Object LOCKER = new Object();

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

    private volatile int barrierCount = THREAD_MAX / 2;
    private CyclicBarrier barrier = new CyclicBarrier(barrierCount);

    public void writeLog(byte topic, short queueId, int offset, ByteBuffer data, Indexer indexer) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (MSG_META_SIZE + msgLen);
        int position;
        try {
            synchronized (LOCKER) {
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
                int arrive = barrier.await(20L * barrierCount, TimeUnit.MILLISECONDS);
                if (arrive == 0) {
                    System.out.println("SNE-F");
                    synchronized (LOCKER) {
                        logMappedBuf.force();
                    }
                }
            } catch (TimeoutException e) {
                System.out.println("TO-F");
                synchronized (LOCKER) {
                    barrier.reset();
                    logMappedBuf.force();
//                    if (barrierCount > 5) {
//                        barrierCount--;
//                        barrier = new CyclicBarrier(barrierCount);
//                    }
                }
            } catch (BrokenBarrierException ignored) {
                // 只有一个超时，其他都是 Broken
            }
            indexer.writeIndex(id, logNumAdder, position, dataSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) throws InterruptedException {
//        CyclicBarrier barrier = new CyclicBarrier(3);
//        Thread thread1 = new Thread(() -> {
//            try {
//                System.out.println("thread1 ");
//                int arrive = barrier.await(1000, TimeUnit.MILLISECONDS);
//                System.out.println("thread1 " + arrive);
//            } catch (BrokenBarrierException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//                barrier.reset();
//            }
//        });
//        thread1.start();
//        Thread thread2 = new Thread(() -> {
//            try {
//                System.out.println("thread2 ");
//                int arrive = barrier.await(5000, TimeUnit.MILLISECONDS);
//                System.out.println("thread2 " + arrive);
//            } catch (BrokenBarrierException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//                barrier.reset();
//            }
//        });
//        thread2.start();
//        Thread thread3 = new Thread(() -> {
//            try {
//                System.out.println("thread3 ");
//                int arrive = barrier.await(10000, TimeUnit.MILLISECONDS);
//                System.out.println("thread3 " + arrive);
//            } catch (BrokenBarrierException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//                barrier.reset();
//            }
//        });
//        thread3.start();
//        Thread.sleep(15_000);
//        thread1 = new Thread(() -> {
//            try {
//                System.out.println("thread1 ");
//                int arrive = barrier.await(1000, TimeUnit.MILLISECONDS);
//                System.out.println("thread1 " + arrive);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        thread1.start();
//        thread2 = new Thread(() -> {
//            try {
//                System.out.println("thread2 ");
//                int arrive = barrier.await(5000, TimeUnit.MILLISECONDS);
//                System.out.println("thread2 " + arrive);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        thread2.start();
//        thread3 = new Thread(() -> {
//            try {
//                System.out.println("thread3 ");
//                int arrive = barrier.await(10000, TimeUnit.MILLISECONDS);
//                System.out.println("thread3 " + arrive);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        thread3.start();
//        Thread.sleep(15_000);
//    }

}
