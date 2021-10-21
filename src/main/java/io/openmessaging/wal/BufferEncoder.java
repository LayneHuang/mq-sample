package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferEncoder {
    public static final Logger log = LoggerFactory.getLogger(BufferEncoder.class);
    private volatile int waitCnt = Constant.DEFAULT_MAX_THREAD_PER_WAL;
    private FileChannel channel = null;
    private MappedByteBuffer buffer = null;
    private final Object LOCK = new Object();
    public final int id;
    public int part;
    public int pos;

    public BufferEncoder(int id) {
        this.id = id;
    }

    public void submit(WalInfoBasic info) throws InterruptedException {
        synchronized (LOCK) {
            // wal 分段
            if (channel == null || pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
                // long tId = Thread.currentThread().getId();
                int prePart = part++;
                try {
                    if (channel != null) {
                        buffer.force();
                        forcedMap.put(prePart, buffer.position());
                        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                        if (cleaner != null) {
                            cleaner.clean();
                        }
                        channel.close();
                    }
                    Files.createFile(Constant.getWALInfoPath(info.walId, part));
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(info.walId, part),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
                    buffer = channel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Constant.WRITE_BEFORE_QUERY
                    );
                    pos = 0;
                    nowWaitCnt = 0;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // log.info("Thread: {}, walId: {}, file: {}, Finished", tId, info.walId, part);
            }
            info.walPart = part;
            info.walPos = pos;
            info.encode(buffer);
            pos += info.getSize();
        }
    }

    private int noFuck = 0;
    private int fuck = 0;
//    private int timeoutTimes = 0;
//    private int fullTimes = 0;

    private final Lock waitLock = new ReentrantLock();
    private final Condition condition = waitLock.newCondition();
    private int nowWaitCnt = 0;

    public void holdOn(WalInfoBasic info) {
        if (waitCnt <= 1) {
            okWrite(info);
            return;
        }
        try {
            waitLock.lock();
            nowWaitCnt++;
            if (nowWaitCnt < waitCnt) {
                condition.await(10L * waitCnt, TimeUnit.MILLISECONDS);
                timeoutWrite(info);
            } else {
                okWrite(info);
                condition.signalAll();
//                if (fullTimes % 10000 == 0) log.info("TIME OVER: {}, FULL: {}, WAIT CNT: {}", timeoutTimes, fullTimes, waitCnt);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            waitLock.unlock();
        }
    }

    private void okWrite(WalInfoBasic info) {
        synchronized (LOCK) {
            if (forced(info)) return;
//            fullTimes++;
            noFuck++;
            if (noFuck > 2 && waitCnt < 20) {
                noFuck = 0;
                fuck = 0;
                waitCnt++;
            }
            buffer.force();
            forcedMap.put(info.walPart, buffer.position());
            nowWaitCnt = 0;
        }
    }

    private void timeoutWrite(WalInfoBasic info) {
        synchronized (LOCK) {
            if (forced(info)) return;
//            timeoutTimes++;
            fuck++;
            if (fuck > 2 && waitCnt >= 2) {
                waitCnt--;
                fuck = 0;
                noFuck = 0;
            }
            buffer.force();
            forcedMap.put(info.walPart, buffer.position());
            nowWaitCnt = 0;
        }
    }

    private final Map<Integer, Integer> forcedMap = new ConcurrentHashMap<>();

    private boolean forced(WalInfoBasic info) {
        int writtenPos = forcedMap.computeIfAbsent(info.walPart, k -> 0);
        return info.getEndPos() <= writtenPos;
    }
}
