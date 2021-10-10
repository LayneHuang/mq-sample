package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class Encoder extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Encoder.class);
    public final BlockingQueue<WalInfoBasic> encodeBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);
    public final BlockingQueue<WritePage> writeBq;
    public final Map<Integer, Idx> IDX;
    private static final Map<Integer, Integer> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public Encoder(BlockingQueue<WritePage> writeBq, Map<Integer, Idx> IDX) {
        this.writeBq = writeBq;
        this.IDX = IDX;
    }

    @Override
    public void run() {
        try {
            while (true) {
                WalInfoBasic info = encodeBq.poll(1, TimeUnit.SECONDS);
                if (info == null) {
                    if (cur == 0) {
                        log.debug("Encoder End");
                        break;
                    }
                    force();
                } else {
                    submit(info);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void submit(WalInfoBasic info) {
        byte[] bs = info.encodeToB();
        info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(info.getKey(), k -> -1) + 1;
        APPEND_OFFSET_MAP.put(info.getKey(), (int) info.pOffset);
        // wal 分段
        if (pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
            force();
            pos = 0;
            part++;
        }
        info.walPart = part;
        info.walPos = pos;
        put(bs);
        // 索引
        Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
        idx.add((int) info.pOffset, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
    }

    private final byte[] tmp = new byte[Constant.WRITE_SIZE];

    private int cur = 0;

    private int pos = 0;

    private int part = 0;

    private int logCount = 0;

    private void put(byte[] bs) {
        if (bs.length == 0) return;
        try {
            lock.lock();
            logCount++;
            for (int i = 0; i < bs.length; ++i) {
                tmp[cur++] = bs[i];
                pos++;
                if (cur == Constant.WRITE_SIZE) {
                    int fullCount = i == bs.length - 1 ? logCount : logCount - 1;
                    writeBq.put(new WritePage(fullCount, part, pos, tmp, cur));
                    cur = 0;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void force() {
        if (cur <= 0) return;
        log.debug("force");
        try {
            writeBq.put(new WritePage(logCount, part, pos, tmp, cur));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cur = 0;
    }

    private ReentrantLock lock = new ReentrantLock();

    public void syncForce() {
        if (cur <= 0) return;
        try {
            lock.lock();
            force();
        } finally {
            lock.unlock();
        }
    }
}
