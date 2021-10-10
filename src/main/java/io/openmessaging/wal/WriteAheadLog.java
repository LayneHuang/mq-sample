package io.openmessaging.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
public class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    private BlockingQueue<WalInfoBasic> writeBq;

    private long logCount;

    public WriteAheadLog() {
    }

    public WriteAheadLog(BlockingQueue<WalInfoBasic> writeBq) {
        this.writeBq = writeBq;
    }

    public WalInfoBasic submit(WalInfoBasic result) {
        result.logCount = ++logCount;
        return result;
    }

    public WalInfoBasic submitEncoder(WalInfoBasic result) {
        try {
            result.logCount = ++logCount;
            writeBq.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }
}
