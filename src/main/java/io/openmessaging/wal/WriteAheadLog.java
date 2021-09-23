package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
public class WriteAheadLog {
    /**
     * 同步水位
     */
    private final WalOffset offset = new WalOffset();
    private final int walId;
    private FileChannel infoChannel;
    private FileChannel valueChannel;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Broker broker;

    public WriteAheadLog(int walId) {
        this.walId = walId;
        this.broker = new Broker(walId, offset);
        initChannels();
        this.broker.start();
    }

    public void initChannels() {
        try {
            infoChannel = FileChannel.open(
                    Constant.getWALPath(walId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            valueChannel = FileChannel.open(
                    Constant.getWALValuePath(walId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int flush(String topic, int queueId, ByteBuffer buffer) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.MSG_SIZE);
        // topic
        infoBuffer.putInt(topicId);
        // queueId
        infoBuffer.putInt(queueId);
        // buffer size
        infoBuffer.putInt(buffer.limit());
        // buffer pos
        infoBuffer.putLong(offset.valueEndOffset);
        infoBuffer.flip();
        // buffer
        buffer.flip();
        lock.writeLock().lock();
        try {
            infoChannel.write(infoBuffer);
            valueChannel.write(buffer);
            offset.logCount.incrementAndGet();
            offset.valueEndOffset = valueChannel.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.writeLock().unlock();
        return walId;
    }

}
