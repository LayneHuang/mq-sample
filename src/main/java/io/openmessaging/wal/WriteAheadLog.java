package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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

    private static final ConcurrentHashMap<String, AtomicLong> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();

    public final BlockingQueue<Long> readBq = new LinkedBlockingQueue<>();

    private ByteBuffer tmpBuffer;

    public WalInfoBasic submit(int topicId, int queueId, ByteBuffer buffer) {
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, buffer);
        lock.writeLock().lock();

        lock.writeLock().unlock();
        result.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(
                WalInfoBasic.getKey(topicId, queueId),
                k -> new AtomicLong()
        ).getAndIncrement();
        return result;
    }

    private void encode(WalInfoBasic msg) {
    }
}
