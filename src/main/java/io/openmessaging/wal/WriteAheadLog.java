package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
public class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    private final Lock lock = new ReentrantLock();

    private static final Map<String, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public final BlockingQueue<WritePage> readBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);

    public WalInfoBasic submit(int topicId, int queueId, ByteBuffer buffer) {
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, buffer);
        String key = WalInfoBasic.getKey(topicId, queueId);
        result.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
        byte[] bs = result.encodeToB();
        lock.lock();
        try {
            // wal 分段
            if (pos + result.getSize() >= Constant.WRITE_BEFORE_QUERY) {
                force();
                pos = 0;
                part++;
            }
            result.walPart = part;
            result.walPos = pos;
            result.submitNum = put(bs);
        } catch (Exception e) {
            log.info(e.toString());
        } finally {
            lock.unlock();
        }
        // 索引
        Idx idx = IDX.computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new Idx());
        idx.add((int) result.pOffset, result.walPart, result.walPos + WalInfoBasic.BYTES, result.valueSize);
        return result;
    }

    private final byte[] tmp = new byte[Constant.WRITE_SIZE];

    private int cur = 0;

    private int pos = 0;

    private int part = 0;
    // 提交时在第几块
    private int submitNum = 0;

    private int put(byte[] bs) {
        try {
            for (byte b : bs) {
                if (cur == Constant.WRITE_SIZE) {
                    readBq.put(new WritePage(part, pos, tmp, cur));
                    submitNum++;
                    cur = 0;
                }
                tmp[cur++] = b;
                pos++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return submitNum;
    }

    public void force() throws InterruptedException {
        if (cur <= 0) {
            return;
        }
        readBq.put(new WritePage(part, pos, tmp, cur));
        submitNum++;
        cur = 0;
    }

    public void syncForce() {
        try {
            lock.lock();
            force();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Idx> IDX = new ConcurrentHashMap<>();

    public static class Idx {
        private static final int IDX_SIZE = 4;
        private static final int BASE_DIS = 16;
        private static final int BASE = (1 << BASE_DIS) - 1;
        private int[] list = new int[128];
        private Lock lock = new ReentrantLock();

        public void add(int pos, int walPart, int walPos, int valueSize) {
            lock.lock();
            int maxPos = pos << 1 | 1;
            if (maxPos + IDX_SIZE > list.length) {
                int[] nList = new int[list.length + (list.length >> 1)];
                System.arraycopy(list, 0, nList, 0, list.length);
                list = nList;
            }
            lock.unlock();
            list[pos << 1] = walPos;
            list[pos << 1 | 1] = ((walPart & BASE) << BASE_DIS) | (valueSize & BASE);
        }

        public int getSize() {
            return list.length;
        }

        public int getWalPart(int pos) {
            int p = (pos << 1) | 1;
//            if (p >= size) log.info("FUCK POS");
            return (list[p] >> BASE_DIS) & BASE;
        }

        public int getWalValueSize(int pos) {
            int p = (pos << 1) | 1;
//            if (p >= size) log.info("FUCK POS");
            return list[p] & BASE;
        }

        public int getWalValuePos(int pos) {
//            int p = pos << 1;
//            if (p >= size) log.info("FUCK POS");
            return list[pos << 1];
        }
    }
}
