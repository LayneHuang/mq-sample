package io.openmessaging.wal;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
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
                    StandardOpenOption.APPEND,
                    StandardOpenOption.DSYNC
            );
            valueChannel = FileChannel.open(
                    Constant.getWALValuePath(walId),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.DSYNC
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int flush(String topic, int queueId, ByteBuffer buffer) {
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic walInfoBasic = new WalInfoBasic(topicId, queueId, buffer.limit());
        ByteBuffer infoBuffer = walInfoBasic.encode();
        infoBuffer.flip();
        // buffer
        // String value = new String(buffer.array());
        // if (value.length() <= 3) log.info("buffer: {}, {}", value, buffer.limit());
        // buffer.flip();
        lock.writeLock().lock();
        try {
            walInfoBasic.pos = offset.msgPos;
            infoChannel.write(infoBuffer);
            valueChannel.write(buffer);
            offset.msgPos += walInfoBasic.size;
            offset.logCount++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        lock.writeLock().unlock();
        try {
            if (walInfoBasic == null) log.info("FUCK");
            bq.put(walInfoBasic);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return walId;
    }

    public void stopBroker() throws InterruptedException {
        bq.put(new WalInfoBasic());
    }
}
