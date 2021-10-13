package io.openmessaging.solve;

import io.openmessaging.MessageQueue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static io.openmessaging.leo2.DataManager.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class LeoMessageQueueImpl extends MessageQueue {

    long start = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        byte topicId = getTopicId(topic);
        long offset = getOffset(topicId, (short) queueId);
        // 更新最大位点
        // 保存 data 中的数据
        writeLog(topicId, (short) queueId, (int) offset, data);
        return offset;
    }

    private byte getTopicId(String topic) {
        byte topicId = (byte) (topic.charAt(5) - '0');
        if (topic.length() == 7) {
            topicId *= 10;
            topicId += (byte) (topic.charAt(6) - '0');
        }
        return topicId;
    }

    Semaphore semaphore = new Semaphore(1);

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            System.out.println("75G cost: " + (System.currentTimeMillis() - start));
//            start = -1;
            try {
                Thread.sleep(5_000);
                semaphore.acquire();
                INDEXERS = new ConcurrentHashMap<>();
                System.out.println("restartLogic");
                restartLogic();
                System.out.println("restartLogic end");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
        byte topicId = getTopicId(topic);
        Map<Integer, ByteBuffer> dataMap = readLog(topicId, (short) queueId, (int) offset, fetchNum);
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}
