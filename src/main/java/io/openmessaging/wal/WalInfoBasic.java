package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * WalInfoBasic
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalInfoBasic {
    private static final Logger log = LoggerFactory.getLogger(WalInfoBasic.class);

    public int topicId;

    public int queueId;

    public int pOffset;

    public int valueSize;

    public long valuePos;

    public int infoPos;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int valueSize, long valuePos) {
        this.valueSize = valueSize;
        this.valuePos = valuePos;
    }

    public WalInfoBasic(int infoPos, int valueSize, long valuePos) {
        this.infoPos = infoPos;
        this.valueSize = valueSize;
        this.valuePos = valuePos;
    }

    public WalInfoBasic(int topicId, int queueId, int valueSize) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = valueSize;
    }

    public WalInfoBasic(int topicId, int queueId, int valueSize, long valuePos) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = valueSize;
        this.valuePos = valuePos;
    }

    public WalInfoBasic(int topicId, int queueId, int pOffset, int valueSize, long valuePos) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.pOffset = pOffset;
        this.valueSize = valueSize;
        this.valuePos = valuePos;
    }

    public ByteBuffer encode() {
        ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.MSG_SIZE);
        return this.encode(infoBuffer);
    }

    public ByteBuffer encode(ByteBuffer infoBuffer) {
        // topic
        infoBuffer.putInt(topicId);
        // queueId
        infoBuffer.putInt(queueId);
        // pOffset
        infoBuffer.putInt(valueSize);
        // buffer size
        infoBuffer.putInt(valueSize);
        // buffer pos
        infoBuffer.putLong(valuePos);
        return infoBuffer;
    }

    public void decode(ByteBuffer buffer) {
        this.topicId = buffer.getInt();
        this.queueId = buffer.getInt();
        this.pOffset = buffer.getInt();
        this.valueSize = buffer.getInt();
        this.valuePos = buffer.getLong();
    }

    public static String getKey(int topicId, int queueId) {
        return topicId + "-" + queueId;
    }

    public String getKey() {
        return topicId + "-" + queueId;
    }

    public String encodeSimple() {
        return valuePos + "-" + valueSize;
    }

    public ByteBuffer encodeSimple(ByteBuffer infoBuffer) {
        infoBuffer.putInt(infoPos);
        // buffer size
        infoBuffer.putInt(valueSize);
        // buffer pos
        infoBuffer.putLong(valuePos);
        return infoBuffer;
    }

    public ByteBuffer decodeSimple(ByteBuffer buffer) {
        this.infoPos = buffer.getInt();
        this.valueSize = buffer.getInt();
        this.valuePos = buffer.getLong();
        return buffer;
    }

    public void show() {
        log.info("topic: {}, queue: {}, pos: {}, size: {}", topicId, queueId, valuePos, valueSize);
    }
}
