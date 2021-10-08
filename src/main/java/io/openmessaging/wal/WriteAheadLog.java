package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
    public final WalOffset offset = new WalOffset();
    private final int walId;
    private FileChannel channel;
    private MappedByteBuffer mapBuffer;
    private final Lock lock = new ReentrantLock();

    public WriteAheadLog(int walId) {
        this.walId = walId;
        initChannels();
    }

    public void initChannels() {
        try {
            channel = FileChannel.open(
                    Constant.getWALInfoPath(walId),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final Map<String, Integer> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public final BlockingQueue<byte[]> readBq = new LinkedBlockingQueue<>();

    public WalInfoBasic submit(int topicId, int queueId, ByteBuffer buffer) {
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, buffer);
        byte[] bs = result.encodeToB();
        lock.lock();
        String key = WalInfoBasic.getKey(topicId, queueId);
        result.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> 0);
        APPEND_OFFSET_MAP.put(key, (int) result.pOffset + 1);
        result.walPos = pos;
        put(bs);
        // 索引
        Idx idx = IDX.computeIfAbsent(WalInfoBasic.getKey(topicId, queueId), k -> new Idx());
        idx.add(result.walPos + WalInfoBasic.BYTES, result.valueSize);
        lock.unlock();
//        log.info("topic: {} , queue: {}, offset:{}, value: {}, pos: {}, size: {}, idxSize: {}",
//                topicId, queueId, result.pOffset, new String(buffer.array()), result.walPos, result.valueSize, idx.size);
        return result;
    }

    private static final int WRITE_SIZE = 4 * 1024;

    private final byte[] tmp = new byte[WRITE_SIZE];

    private int cur = 0;

    private int pos = 0;

    private void put(byte[] bs) {
        try {
            for (byte b : bs) {
                if (cur == WRITE_SIZE) {
                    readBq.put(tmp);
                    cur = 0;
                }
                tmp[cur++] = b;
            }
            pos += bs.length;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void force() {
        lock.lock();
        if (cur > 0) {
            byte[] cTmp = new byte[cur];
            System.arraycopy(tmp, 0, cTmp, 0, cur);
            cur = 0;
            try {
                readBq.put(cTmp);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.unlock();
    }

    public Map<String, Idx> IDX = new ConcurrentHashMap<>();

    public static class Idx {
        public static final int IDX_SIZE = 4;
        public int[] list = new int[1024];
        public int size = 0;

        public void add(int walPos, int valueSize) {
            while (size + IDX_SIZE > list.length) {
                int[] nList = new int[list.length + (list.length >> 1)];
                if (size >= 0) System.arraycopy(list, 0, nList, 0, size);
                list = nList;
            }
            list[size++] = walPos;
            list[size++] = valueSize;
        }
    }
}
