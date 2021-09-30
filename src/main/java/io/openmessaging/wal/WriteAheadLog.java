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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private MappedByteBuffer infoMapBuffer;

    private MappedByteBuffer valueMapBuffer;

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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flush(String topic, int queueId, ByteBuffer buffer) {
        int logCount = 0;
        int topicId = IdGenerator.getId(topic);
        WalInfoBasic walInfoBasic = new WalInfoBasic(topicId, queueId, buffer.limit());
        lock.writeLock().lock();
        try {
            walInfoBasic.valuePos = offset.valuePos;
            putInfo(walInfoBasic);
            putValue(buffer);
            offset.infoPos += Constant.MSG_SIZE;
            offset.valuePos += walInfoBasic.valueSize;
            offset.logCount.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.writeLock().unlock();
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
        if (valueMapBuffer == null || !valueMapBuffer.hasRemaining()) {
            valueMapBuffer = valueChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    offset.valuePos,
                    Constant.WAL_BUFFER_SIZE
            );
        }
        valueMapBuffer.put(buffer);
        valueMapBuffer.force();
    }
}
