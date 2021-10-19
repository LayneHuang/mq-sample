package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * WriteAheadLog
 *
 * @author layne
 * @since 2021/9/17
 */
@Deprecated
public class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    public BlockingQueue<WalInfoBasic> logsBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);

    private long logCount;

    public WriteAheadLog() {
    }

    public void submit(WalInfoBasic result) {
        try {
            result.logCount = ++logCount;
            logsBq.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
