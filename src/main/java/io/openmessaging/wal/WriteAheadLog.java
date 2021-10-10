package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
public class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    public final BlockingQueue<WalInfoBasic> writeBq;

    private int logCount;

    public WriteAheadLog(BlockingQueue<WalInfoBasic> writeBq) {
        this.writeBq = writeBq;
    }

    public synchronized WalInfoBasic submit(int topicId, int queueId, ByteBuffer buffer) {
        WalInfoBasic result = new WalInfoBasic(topicId, queueId, buffer);
        result.logCount = ++logCount;
        try {
            log.debug("wal {}, submit: {}", topicId % Constant.WAL_FILE_COUNT, logCount);
            writeBq.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }
}
