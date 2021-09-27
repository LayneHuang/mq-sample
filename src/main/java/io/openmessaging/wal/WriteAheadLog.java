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
    private final WalOffset offset = new WalOffset();
    private final int walId;
    private FileChannel infoChannel;
    private FileChannel valueChannel;
    private final BlockingQueue<WalInfoBasic> bq = new LinkedBlockingDeque<>(Constant.LOG_SEGMENT_SIZE);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Broker broker;

    private MappedByteBuffer infoMapBuffer;

    private MappedByteBuffer valueMapBuffer;

    public WriteAheadLog(int walId) {
        this.walId = walId;
        initChannels();
        this.broker = new Broker(walId, offset, bq);
        this.broker.start();
    }

    public void initChannels() {
        try {
            infoChannel = FileChannel.open(
                    Constant.getWALPath(walId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            valueChannel = FileChannel.open(
                    Constant.getWALValuePath(walId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int flush(String topic, int queueId, ByteBuffer buffer) {
        int logCount = 0;
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic walInfoBasic = new WalInfoBasic(topicId, queueId, buffer.limit());
        ByteBuffer infoBuffer = walInfoBasic.encode();
        infoBuffer.flip();
        lock.writeLock().lock();
        try {
            walInfoBasic.pos = offset.msgPos;
            infoChannel.write(infoBuffer);
            valueChannel.write(buffer);
            offset.msgPos += walInfoBasic.size;
            logCount = ++offset.logCount;
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.writeLock().unlock();
        try {
            bq.put(walInfoBasic);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return logCount;
    }

    private void putInfo() throws IOException {
        if (infoMapBuffer == null || !infoMapBuffer.hasRemaining()) {
            infoMapBuffer = infoChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.WAL_BUFFER_SIZE);
        }
    }

    public void stopBroker() throws InterruptedException {
        bq.put(new WalInfoBasic());
    }
}
