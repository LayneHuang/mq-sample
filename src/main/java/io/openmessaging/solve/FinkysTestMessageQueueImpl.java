package io.openmessaging.solve;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.openmessaging.MessageQueue;

import static io.openmessaging.finkys.DataManager.getOffset;
import static io.openmessaging.finkys.DataManager.readLog;
import static io.openmessaging.finkys.DataManager.writeLog;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class FinkysTestMessageQueueImpl extends MessageQueue {

    private static final AtomicLong showLog = new AtomicLong();
    //    private static final AtomicLong showLog = new AtomicLong();
    private long startTime = 0;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        if (startTime == 0){
            startTime = System.currentTimeMillis();
        }
        int topicHash = topic.hashCode();
        String key = (topicHash + " + " + queueId).intern();
        long offset = getOffset(key);
        // 更新最大位点
        // 保存 data 中的数据
        writeLog(topicHash, queueId, offset, data);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (showLog.getAndIncrement() == 0){
            System.out.println("第一阶段耗时:"+(System.currentTimeMillis() - startTime)+"ms");
        }else {
            return Collections.emptyMap();
        }
//        if (showLog.getAndIncrement() == 0){
//            System.out.println("第一阶段耗时:"+(System.currentTimeMillis() - startTime)+"ms");
//        }else {
//            return Collections.emptyMap();
//        }
        int topicHash = topic.hashCode();
        Map<Integer, ByteBuffer> dataMap = readLog(topicHash, queueId, offset, fetchNum);
        if (dataMap != null) {
            return dataMap;
        } else {
            return Collections.emptyMap();
        }
    }
}