package io.openmessaging.solve;

import io.openmessaging.MessageQueue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.leo.DataManager.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class LeoMessageQueueImpl extends MessageQueue {

    long start = 0;

    Set<String> topicSet = new HashSet<>(100);

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        if (!topicSet.contains(topic)) {
            topicSet.add(topic);
            System.out.println("tpc " + topic);
        }
        if (queueId > Short.MAX_VALUE) {
            System.out.println("queueId " + queueId);
        }
        int topicHash = topic.hashCode();
        String key = (topicHash + " + " + queueId).intern();
        long offset = getOffset(key);
        // 更新最大位点
        // 保存 data 中的数据
        writeLog(topicHash, queueId, (int) offset, data);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            System.out.println("75G cost: " + (System.currentTimeMillis() - start));
            start = -1;
        }
        int topicHash = topic.hashCode();
        Map<Integer, ByteBuffer> dataMap = readLog(topicHash, queueId, (int) offset, fetchNum);
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}
