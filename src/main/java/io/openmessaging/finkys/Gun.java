package io.openmessaging.finkys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import static io.openmessaging.finkys.BulletManager.LOGS_PATH;

public class Gun extends Thread {

    private static final int FLUSH_SIZE = 16 * 1024;
    private static final int MERGE_SIZE = 10;

    private byte id;
    private Path logDir;
    private byte logNumAdder = Byte.MIN_VALUE;
    private FileChannel logFileChannel;
    private MappedByteBuffer logMappedBuf;
    private final Object LOCKER = new Object();
    private CyclicBarrier barrier;
    public LinkedBlockingQueue<Bullet> clip = new LinkedBlockingQueue<>();


    public Gun(byte id) {
        this.id = id;
        logDir = LOGS_PATH.resolve(String.valueOf(this.id));
        barrier = new CyclicBarrier(MERGE_SIZE);
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
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);
    }

    private int currentCount = 0;
    private boolean mergeIO = true;

    public void append(int topic,int queueId,long offset,ByteBuffer data){
        BulletIndexer indexer = BulletManager.INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new BulletIndexer(topic, queueId));
        short msgLen = (short) data.limit();
        short dataSize = (short) (18 + msgLen);
        if (mergeIO){
            try {
                synchronized (LOCKER){
                    if (logMappedBuf.remaining() < dataSize) {
                        currentCount = 0;
                        currentSize = 0;
                        logMappedBuf.force();
                        unmap(logMappedBuf);
                        logFileChannel.close();
                        openLog();
                        barrier.reset();
                    }
                    currentSize += dataSize;
                    currentCount ++;
                    int position = logMappedBuf.position();
                    logMappedBuf.putInt(topic); // 4
                    logMappedBuf.putInt(queueId); // 4
                    logMappedBuf.putLong(offset); // 8
                    logMappedBuf.putShort(msgLen); // 2
                    logMappedBuf.put(data);
                    if (currentCount >= MERGE_SIZE || currentSize >= FLUSH_SIZE){
                        currentCount = 0;
                        currentSize = 0;
                        logMappedBuf.force();
                    }
                    // index
                    indexer.writeIndex(id, logNumAdder, position, dataSize);
                }
                try {
                    barrier.await(1000,TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    synchronized (LOCKER){
                        if (mergeIO){
                            mergeIO = false;
                            logMappedBuf.force();
                        }
                    }
//                MERGE_SIZE = currentCount;
//                barrier = new CyclicBarrier(MERGE_SIZE);
                } catch (Exception e){

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            try {
                synchronized (LOCKER){
                    if (logMappedBuf.remaining() < dataSize) {
                        logMappedBuf.force();
                        unmap(logMappedBuf);
                        logFileChannel.close();
                        openLog();
                    }
                    int position = logMappedBuf.position();
                    logMappedBuf.putInt(topic); // 4
                    logMappedBuf.putInt(queueId); // 4
                    logMappedBuf.putLong(offset); // 8
                    logMappedBuf.putShort(msgLen); // 2
                    logMappedBuf.put(data);
                    logMappedBuf.force();
                    // index
                    indexer.writeIndex(id, logNumAdder, position, dataSize);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            System.out.println("GUN-"+id+": start!");
            while (true) {
                Bullet bullet = clip.poll(50, TimeUnit.MILLISECONDS);
//                Bullet bullet = BulletManager.clip.poll(50, TimeUnit.MILLISECONDS);
                if (bullet == null) {
                    fire();
                    continue;
                }
                bullets.add(bullet);
                int topic = bullet.getTopicHash();
                int queueId = bullet.getQueueId();
                long offset = bullet.getOffset();
                ByteBuffer data = bullet.getData();
                BulletIndexer indexer = BulletManager.INDEXERS.computeIfAbsent(topic + "+" + queueId, k -> new BulletIndexer(topic, queueId));
                short msgLen = (short) data.limit();
                short dataSize = (short) (18 + msgLen);
                currentSize += dataSize;
                synchronized (indexer.LOCKER) {
                    try {
                        if (logMappedBuf.remaining() < dataSize) {
                            fire();
                            unmap(logMappedBuf);
                            logFileChannel.close();
                            openLog();
                        }
                        int position = logMappedBuf.position();
                        logMappedBuf.putInt(topic); // 4
                        logMappedBuf.putInt(queueId); // 4
                        logMappedBuf.putLong(offset); // 8
                        logMappedBuf.putShort(msgLen); // 2
                        logMappedBuf.put(data);
                        if (currentSize >= FLUSH_SIZE) {
                            fire();
                        }
                        // index
                        indexer.writeIndex(id, logNumAdder, position, dataSize);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int currentSize = 0;
    private List<Bullet> bullets = new ArrayList<>();

    /**
     * 开火
     */
    private void fire() {
        currentSize = 0;
        logMappedBuf.force();
        for (Bullet bullet : bullets) {
            bullet.release();
        }
        bullets.clear();
    }

    private static void unmap(MappedByteBuffer indexMapBuf) {
        Cleaner cleaner = ((DirectBuffer) indexMapBuf).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }
}
