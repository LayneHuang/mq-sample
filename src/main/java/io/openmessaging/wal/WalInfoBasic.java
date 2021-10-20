package io.openmessaging.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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

    public int walId;

    public int walPart;

    public int walPos;

    public long logCount;

    public ByteBuffer value;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int pOffset, int walPart, int walPos, int valueSize) {
        this.pOffset = pOffset;
        this.walPart = walPart;
        this.walPos = walPos;
        this.valueSize = valueSize;
    }

    public WalInfoBasic(int topicId, int queueId, ByteBuffer value) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = value.limit();
        this.value = value;
    }

    public static final int BYTES = 7;

    public void encode(MappedByteBuffer buffer) {
        // topicId
        buffer.put((byte) topicId);
        // queueId
        buffer.put((byte) ((queueId >> 8) & 0xff));
        buffer.put((byte) (queueId & 0xff));
        // pOffset
        buffer.put((byte) ((pOffset >> 8) & 0xff));
        buffer.put((byte) (pOffset & 0xff));
        // value
        buffer.put((byte) ((valueSize >> 8) & 0xff));
        buffer.put((byte) (valueSize & 0xff));
        // value
        buffer.put(value);
    }

    public void decode(ByteBuffer buffer, boolean hasValue) {
        // topicId
        topicId = buffer.get() & 0xff;
        // queueId
        queueId = buffer.get() & 0xff;
        queueId <<= 8;
        queueId |= buffer.get() & 0xff;
        // pOffset
        pOffset = buffer.get() & 0xff;
        pOffset <<= 8;
        pOffset |= buffer.get() & 0xff;
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

    public int getEndPos() {
        return walPos + getSize();
    }

    public static int getKey(int topicId, int queueId) {
        return queueId * 100 + (topicId - 1);
    }

    public int getKey() {
        return queueId * 100 + (topicId - 1);
    }
}
