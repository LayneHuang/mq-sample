package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static io.openmessaging.leo.DataManager.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        String key = (topic + " + " + queueId).intern();
        long offset = getOffset(key);
        // 更新最大位点
        // 保存 data 中的数据
        writeLog(topic, queueId, data);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> dataMap = readLog(topic, queueId, offset, fetchNum);
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}
