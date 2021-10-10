package io.openmessaging.solve;

import io.openmessaging.Constant;
import io.openmessaging.IdGenerator;
import io.openmessaging.MessageQueue;
import io.openmessaging.wal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LayneMessageQueueImpl extends MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(LayneMessageQueueImpl.class);

    private static final WriteAheadLog[] walList = new WriteAheadLog[Constant.WAL_FILE_COUNT];

    private static final Broker[] brokers = new Broker[Constant.WAL_FILE_COUNT];
    //    private static final Helper[] helpers = new Helper[Constant.WAL_FILE_COUNT];
    private static final Encoder[] encoders = new Encoder[Constant.WAL_FILE_COUNT];

    private static final Loader[] loader = new Loader[Constant.WAL_FILE_COUNT];

    public Map<Integer, Idx> IDX = new ConcurrentHashMap<>();

    public LayneMessageQueueImpl() {
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            brokers[i] = new Broker(i);
            encoders[i] = new Encoder(brokers[i].writeBq, IDX);
            walList[i] = new WriteAheadLog(encoders[i].encodeBq);
//            helpers[i] = new Helper(i);
            encoders[i].start();
            brokers[i].start();
        }
    }

    private void reload() {
        if (!IdGenerator.load()) {
            return;
        }
        for (int i = 0; i < Constant.WAL_FILE_COUNT; ++i) {
            loader[i] = new Loader(i, walList[i]);
            loader[i].start();
        }
    }

    private long start = 0;
    private int queryCnt = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        WalInfoBasic submitResult = walList[walId].submit(topicId, queueId, data);
        int wait = 0;
        while (submitResult.logCount > brokers[walId].logCount.get()) {
            wait++;
            if (wait > 2000) encoders[walId].syncForce();
        }
        long cost = System.currentTimeMillis() - start;
        if (cost > 10 * 60 * 1000) {
            log.info("time over: {}", submitResult.logCount);
            return 0;
        }
        return submitResult.pOffset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            log.info("75G cost: " + (System.currentTimeMillis() - start));
        }
        queryCnt++;
        if (queryCnt > 3) return null;
        int topicId = IdGenerator.getId(topic);
        int walId = topicId % Constant.WAL_FILE_COUNT;
        Idx idx = IDX.get(WalInfoBasic.getKey(topicId, queueId));
        return readValueFromWAL(walId, (int) offset, fetchNum, idx);
    }

    private Map<Integer, ByteBuffer> readValueFromWAL(int walId, int offset, int fetchNum, Idx idx) {
        Map<Integer, ByteBuffer> result = new HashMap<>(fetchNum);
        FileChannel valueChannel = null;
        int curPart = -1;
        try {
            for (int i = 0; i < fetchNum; ++i) {
                int key = offset + i;
                if ((key << 1) >= idx.getSize()) continue;
                int part = idx.getWalPart(key);
                if (valueChannel == null || part != curPart) {
                    if (valueChannel != null) {
                        valueChannel.close();
                    }
                    valueChannel = FileChannel.open(
                            Constant.getWALInfoPath(walId, part),
                            StandardOpenOption.READ);
                    curPart = part;
                }
                int pos = idx.getWalValuePos(key);
                int size = idx.getWalValueSize(key);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                valueChannel.read(buffer, pos);
                buffer.flip();
                result.put(i, buffer);
                log.debug("key: {}, value: {}, pos: {}, size: {}", key, new String(buffer.array()), pos, size);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
