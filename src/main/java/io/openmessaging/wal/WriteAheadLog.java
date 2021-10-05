package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
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
    private FileChannel infoChannel;
    private FileChannel valueChannel;
    private MappedByteBuffer infoMapBuffer;
    private MappedByteBuffer valueMapBuffer;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, ByteBuffer> walIndex = new HashMap<>();

    public WriteAheadLog(int walId) {
        this.walId = walId;
        initChannels();
    }

    public void initChannels() {
        try {
            infoChannel = FileChannel.open(
                    Constant.getWALInfoPath(walId),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
            valueChannel = FileChannel.open(
                    Constant.getWALValuePath(walId),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
            offset.infoPos = infoChannel.position();
            offset.valuePos = valueChannel.position();
            log.info("init info pos: {}, value pos: {}", offset.infoPos, offset.valuePos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flush(String topic, int queueId, ByteBuffer buffer, long pOffset) {
        int topicId = IdGenerator.getId(topic);
        WalInfoBasic walInfoBasic = new WalInfoBasic(topicId, queueId, buffer.limit());
        long infoPos = 0;
        lock.writeLock().lock();
        try {
            walInfoBasic.valuePos = offset.valuePos;
            putInfo(walInfoBasic);
            putValue(buffer);
            infoPos = offset.infoPos;
            offset.infoPos += Constant.MSG_SIZE;
            offset.valuePos += walInfoBasic.valueSize;
            offset.logCount.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.writeLock().unlock();
        // 间距100插入一索引, page内不冲突, 可以无需同步
        if (pOffset % Constant.INDEX_DISTANCE == 0) {
            saveIndex(topicId, queueId, infoPos);
        }
    }

    private void putInfo(WalInfoBasic walInfoBasic) throws IOException {
        if (infoMapBuffer == null || !infoMapBuffer.hasRemaining()) {
            infoMapBuffer = infoChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset.infoPos,
                    Constant.WAL_BUFFER_SIZE
            );
        }
//        walInfoBasic.show();
        infoMapBuffer = (MappedByteBuffer) walInfoBasic.encode(infoMapBuffer);
        infoMapBuffer.force();
    }

    private void putValue(ByteBuffer buffer) throws IOException {
        if (valueMapBuffer == null
                || valueMapBuffer.remaining() < buffer.limit()) {
            valueMapBuffer = valueChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset.valuePos,
                    Constant.WAL_BUFFER_SIZE
            );
        }
        valueMapBuffer.put(buffer);
        valueMapBuffer.force();
    }

    private void saveIndex(int topicId, int queueId, long walPos) {
        ByteBuffer buffer = walIndex.computeIfAbsent(
                WalInfoBasic.getKey(topicId, queueId),
                k -> ByteBuffer.allocate(Constant.INDEX_CACHE_SIZE)
        );
        buffer.putLong(walPos);
        if (!buffer.hasRemaining()) {
            log.info("save idx, walPos: {}", walPos);
            try (FileChannel channel = FileChannel.open(Constant.getWALIndexPath(topicId, queueId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.DSYNC
            )) {
                channel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
