package io.openmessaging.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

    public int walPart;

    public int walPos;

    public int submitNum;

    public ByteBuffer value;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int topicId, int queueId, ByteBuffer value) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = value.limit();
        this.value = value;
    }

    public static final int BYTES = 1 + 2 * 2;

    public byte[] encodeToB() {
        byte[] result = new byte[BYTES + this.valueSize];
        // topicId
        result[0] = (byte) topicId;
        // queueId
        result[1] = (byte) ((queueId >> 8) & 0xff);
        result[2] = (byte) (queueId & 0xff);
        // value size
        result[3] = (byte) ((valueSize >> 8) & 0xff);
        result[4] = (byte) (valueSize & 0xff);
        // value
        System.arraycopy(value.array(), 0, result, BYTES, result.length - BYTES);
        return result;
    }

    public void decode(ByteBuffer buffer, boolean hasValue) {
        // topicId
        topicId = buffer.get() & 0xff;
        // queueId
        queueId = buffer.get() & 0xff;
        queueId <<= 8;
        queueId |= buffer.get() & 0xff;
        // valueSize
        valueSize = buffer.get() & 0xff;
        valueSize <<= 8;
        valueSize |= buffer.get() & 0xff;
        // value
        if (!hasValue) {
            return;
        }
        value = ByteBuffer.allocate(valueSize);
        for (int i = 0; i < valueSize; ++i) {
            value.put(buffer.get());
        }
    }

    public int getSize() {
        return BYTES + this.valueSize;
    }

    public static String getKey(int topicId, int queueId) {
        return topicId + "-" + queueId;
    }

    public String getKey() {
        return topicId + "-" + queueId;
    }

}
