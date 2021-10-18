package io.openmessaging.solve;

import io.openmessaging.MessageQueue;
import io.openmessaging.leo2.DataManager;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static io.openmessaging.leo2.DataManager.getOffset;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class LeoMessageQueueImpl extends MessageQueue {

    DataManager manager = new DataManager();

    long start = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (start == 0) {
            start = System.currentTimeMillis();
        }
        byte topicId = getTopicId(topic);
        int offset = getOffset(topicId, (short) queueId);
        // 更新最大位点
        // 保存 data 中的数据
        manager.writeLog(topicId, (short) queueId, offset, data);
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

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (start != -1) {
            System.out.println("75G cost: " + (System.currentTimeMillis() - start));
            start = -1;
//            return null;
        }
        byte topicId = getTopicId(topic);
        Map<Integer, ByteBuffer> dataMap = manager.readLog(topicId, (short) queueId, (int) offset, fetchNum);
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}
