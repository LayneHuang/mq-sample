package io.openmessaging.wal;

import io.openmessaging.Constant;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferEncoder {
    private volatile int waitCnt = Constant.DEFAULT_MAX_THREAD_PER_WAL;
    private FileChannel channel = null;
    private MappedByteBuffer buffer = null;
    private final Object LOCK = new Object();
    public final int id;
    public int part;
    public int pos;
    public Cache cache;

    public BufferEncoder(int id) {
        this.id = id;
        this.cache = new Cache(id);
        try {
            channel = FileChannel.open(
                    Constant.getWALInfoPath(id, part),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            buffer = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Constant.WRITE_BEFORE_QUERY
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void submit(WalInfoBasic info) {
        synchronized (LOCK) {
            // wal 分段
            if (pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
                int prePart = part++;
                try {
                    buffer.force();
                    forcedMap.put(prePart, buffer.position());
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) {
                        cleaner.clean();
                    }
                    channel.close();
                    channel = FileChannel.open(
                            Constant.getWALInfoPath(info.walId, part),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE);
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
            }
            info.walPart = part;
            info.walPos = pos;
            info.encode(buffer);
            pos += info.getSize();
        }
    }

    private int noFuck = 0;
    private int fuck = 0;

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
                // 后写线程先唤醒
                long waitT = 5L * (waitCnt - nowWaitCnt);
                condition.await(waitT, TimeUnit.MILLISECONDS);
                timeoutWrite(info);
            } else {
                condition.signalAll();
                okWrite(info);
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
            noFuck++;
            if (noFuck > 2 && waitCnt < Constant.DEFAULT_MAX_THREAD_PER_WAL) {
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
