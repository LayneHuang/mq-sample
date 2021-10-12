package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BrokerManager
 *
 * @author 86188
 * @since 2021/10/12
 */
@Deprecated
public class BrokerManager extends Thread {
    public static final Logger log = LoggerFactory.getLogger(BrokerManager.class);
    private final List<BlockingQueue<WritePage>> producer;

    private final List<WriteSource> sources = new ArrayList<>();
    private final Lock[] locks;
    private final Condition[] conditions;
    private final ExecutorService executor = new ThreadPoolExecutor(
            Constant.WAL_FILE_COUNT,
            Constant.WAL_FILE_COUNT * 3,
            200,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(Constant.BQ_SIZE)
    );

    public BrokerManager(List<BlockingQueue<WritePage>> producer,
                         Lock[] locks,
                         Condition[] conditions) {
        this.producer = producer;
        this.locks = locks;
        this.conditions = conditions;
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            sources.add(new WriteSource(i));
        }
    }

    @Override
    public void run() {
        int nullCnt = 0;
        int curWalId = 0;
        int producerSize = producer.size();
        try {
            while (true) {
                curWalId = (curWalId + 1) % producerSize;
                WritePage info = producer.get(curWalId).poll(20, TimeUnit.MILLISECONDS);
                if (info == null) {
                    nullCnt++;
                    if (nullCnt > 200) {
                        log.info("NO TASK");
                        break;
                    }
                    continue;
                }
                nullCnt = 0;
                WriteSource source = sources.get(curWalId);
                Lock lock = locks[curWalId];
                Condition condition = conditions[curWalId];
                executor.execute(() -> {
                    try {
                        source.write(info);
                        signal(lock, condition);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    public boolean isDown(int walId, long logCount) {
        return logCount <= sources.get(walId).logCount.get();
    }

    private void signal(Lock lock, Condition condition) {
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private static class WriteSource {
        private final int walId;
        private FileChannel channel = null;
        private MappedByteBuffer buffer = null;
        private int curPart = 0;
        // 公平锁, 先调用的先写
        private final Lock lock = new ReentrantLock(true);
        // 已经落盘的log编号
        public AtomicLong logCount = new AtomicLong();

        public WriteSource(int walId) {
            this.walId = walId;
        }

        public void write(WritePage page) throws IOException {
            lock.lock();
            if (channel == null || page.part != curPart) {
                if (channel != null) {
                    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                    if (cleaner != null) {
                        cleaner.clean();
                    }
                    channel.close();
                }
                channel = FileChannel.open(
                        Constant.getWALInfoPath(walId, page.part),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
                buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Constant.WRITE_BEFORE_QUERY
                );
                curPart = page.part;
            }
            buffer.put(page.value);
//            if (page.logCount > 50 && page.logCount < 100 && walId == 4) {
//                log.info("WRITE LOG WAL: {}, PART: {}, CNT: {}", walId, page.part, page.logCount);
//            }
            buffer.force();
            logCount.set(page.logCount);
            lock.unlock();
        }
    }
}