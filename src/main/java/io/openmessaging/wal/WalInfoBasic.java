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

    public long pOffset;

    public int valueSize;

    public int walPos;

    public ByteBuffer value;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int topicId, int queueId) {
        this.topicId = topicId;
        this.queueId = queueId;
    }

    public WalInfoBasic(int topicId, int queueId, int valueSize) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = valueSize;
    }

    public WalInfoBasic(int topicId, int queueId, ByteBuffer value) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = value.limit();
        this.value = value;
    }

    public ByteBuffer encode() {
        ByteBuffer infoBuffer = ByteBuffer.allocate(Constant.MSG_SIZE);
        return this.encode(infoBuffer);
    }

    public static final int BYTES = 1 + 2 + 4;

    public byte[] encodeToB() {
        byte[] result = new byte[BYTES + this.valueSize];
        // topicId
        result[0] = (byte) topicId;
        // queueId
        result[1] = (byte) ((queueId >> 4) & 0xff);
        result[2] = (byte) (queueId & 0xff);
        // value size
        result[3] = (byte) ((valueSize >> 12) & 0xff);
        result[4] = (byte) ((valueSize >> 8) & 0xff);
        result[5] = (byte) ((valueSize >> 4) & 0xff);
        result[6] = (byte) (valueSize & 0xff);
        // value
        System.arraycopy(value.array(), 0, result, BYTES, result.length - BYTES);
        return result;
    }

    public ByteBuffer encode(ByteBuffer infoBuffer) {
        // topic
        infoBuffer.putInt(topicId);
        // queueId
        infoBuffer.putInt(queueId);
        // infoPos
//        infoBuffer.putInt(infoPos);
        // pOffset
        infoBuffer.putInt(valueSize);
        // buffer size
        infoBuffer.putInt(valueSize);
        // buffer pos
//        infoBuffer.putLong(valuePos);
        // buffer
        infoBuffer.put(value);
        return infoBuffer;
    }

    public void decode(ByteBuffer buffer) {
        this.topicId = buffer.getInt();
        this.queueId = buffer.getInt();
//        this.infoPos = buffer.getInt();
        this.pOffset = buffer.getInt();
        this.valueSize = buffer.getInt();
//        this.valuePos = buffer.getLong();
    }

    public static String getKey(int topicId, int queueId) {
        return topicId + "-" + queueId;
    }

    public String getKey() {
        return topicId + "-" + queueId;
    }

}
