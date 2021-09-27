package io.openmessaging;

import io.openmessaging.solve.LayneMessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    MessageQueue result = new LayneMessageQueueImpl();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return result.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return result.getRange(topic, queueId, offset, fetchNum);
    }
}
