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
    /**
     * 同步水位
     */
    private final Lock lock = new ReentrantLock();

    private static final Map<String, AtomicInteger> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public final BlockingQueue<WritePage> readBq = new LinkedBlockingQueue<>();

    public WalInfoBasic submit(int topicId, int queueId, ByteBuffer buffer) {
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, buffer);
        byte[] bs = result.encodeToB();
        lock.lock();
        try {
            String key = WalInfoBasic.getKey(topicId, queueId);
            result.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
            // wal 分段
            if (pos + result.getSize() >= Constant.WRITE_BEFORE_QUERY) {
                pos = 0;
                part++;
            }
            result.walPart = part;
            result.walPos = pos;
            result.submitNum = submitNum;
            put(bs);
            // 索引
            Idx idx = IDX.computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new Idx());
            idx.add(result.walPart, result.walPos + WalInfoBasic.BYTES, result.valueSize);
        } catch (Exception e) {
            log.info(e.toString());
        } finally {
            lock.unlock();
        }
        return result;
    }

    private static final int WRITE_SIZE = 64 * 1024;

    private final byte[] tmp = new byte[WRITE_SIZE];

    private int cur = 0;

    private int pos = 0;

    private int part = 0;

    private int submitNum = 0;

    private void put(byte[] bs) {
        try {
            for (byte b : bs) {
                if (cur == WRITE_SIZE) {
                    readBq.put(new WritePage(part, pos, tmp));
                    submitNum++;
                    cur = 0;
                }
                tmp[cur++] = b;
                pos++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void force() {
        if (cur <= 0) {
            return;
        }
        lock.lock();
        try {
            byte[] cTmp = new byte[cur];
            System.arraycopy(tmp, 0, cTmp, 0, cur);
            readBq.put(new WritePage(part, pos, cTmp));
            submitNum++;
            pos += cur;
            cur = 0;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public Map<String, Idx> IDX = new ConcurrentHashMap<>();

    public static class Idx {
        public static final int IDX_SIZE = 4;
        private static final int BASE_DIS = 16;
        private static final int BASE = (1 << BASE_DIS) - 1;
        public int[] list = new int[1024];
        public int size = 0;

        public void add(int walPart, int walPos, int valueSize) {
            while (size + IDX_SIZE > list.length) {
                int[] nList = new int[list.length + (list.length >> 1)];
                if (size >= 0) System.arraycopy(list, 0, nList, 0, size);
                list = nList;
            }
            list[size++] = walPos;
            list[size++] = ((walPart & BASE) << BASE_DIS) | (valueSize & BASE);
        }

        public int getWalPart(int pos) {
            return (list[(pos << 1) | 1] >> BASE_DIS) & BASE;
        }

        public int getWalValueSize(int pos) {
            return list[(pos << 1) | 1] & BASE;
        }

        public int getWalValuePos(int pos) {
            return list[pos << 1];
        }
    }
}
