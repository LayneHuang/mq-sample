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

    public WalInfoBasic(int pOffset, int walId, int walPart, int walPos, int valueSize) {
        this.pOffset = pOffset;
        this.walId = walId;
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

    public static final int BYTES = 1 + 2 * 3;

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

    public byte[] encodeToB() {
        byte[] result = new byte[BYTES + this.valueSize];
        // topicId
        result[0] = (byte) topicId;
        // queueId
        result[1] = (byte) ((queueId >> 8) & 0xff);
        result[2] = (byte) (queueId & 0xff);
        // pOffset
        result[3] = (byte) ((pOffset >> 8) & 0xff);
        result[4] = (byte) (pOffset & 0xff);
        // value size
        result[5] = (byte) ((valueSize >> 8) & 0xff);
        result[6] = (byte) (valueSize & 0xff);
        // value
        System.arraycopy(value.array(), 0, result, BYTES, result.length - BYTES);
        return result;
    }

    private byte checkAndGetByte(FileChannel channel, ByteBuffer buffer) {
        if (buffer.hasRemaining()) return buffer.get();
        buffer.clear();
        try {
            channel.read(buffer);
            buffer.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.get();
    }

    public void decodeSafety(FileChannel channel, ByteBuffer buffer, boolean hasValue) {
        // topicId
        topicId = checkAndGetByte(channel, buffer) & 0xff;
        // queueId
        queueId = checkAndGetByte(channel, buffer) & 0xff;
        queueId <<= 8;
        queueId |= checkAndGetByte(channel, buffer) & 0xff;
        // pOffset
        pOffset = checkAndGetByte(channel, buffer) & 0xff;
        pOffset <<= 8;
        pOffset |= checkAndGetByte(channel, buffer) & 0xff;
        // valueSize
        valueSize = checkAndGetByte(channel, buffer) & 0xff;
        valueSize <<= 8;
        valueSize |= checkAndGetByte(channel, buffer) & 0xff;
        // value
        if (!hasValue) {
            return;
        }
        value = ByteBuffer.allocate(valueSize);
        for (int i = 0; i < valueSize; ++i) {
            value.put(checkAndGetByte(channel, buffer));
        }
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

    public int decode(ByteBuffer buffer, int pos) {
        // topicId
        topicId = buffer.get(pos) & 0xff;
        // queueId
        queueId = buffer.get(pos + 1) & 0xff;
        queueId <<= 8;
        queueId |= buffer.get(pos + 2) & 0xff;
        // pOffset
        pOffset = buffer.get(pos + 3) & 0xff;
        pOffset <<= 8;
        pOffset |= buffer.get(pos + 4) & 0xff;
        // valueSize
        valueSize = buffer.get(pos + 5) & 0xff;
        valueSize <<= 8;
        valueSize |= buffer.get(pos + 6) & 0xff;
        return pos + valueSize;
    }

    public int getSize() {
        return BYTES + this.valueSize;
    }

    public static int getKey(int topicId, int queueId) {
        return queueId * 100 + (topicId - 1);
    }

    public int getKey() {
        return queueId * 100 + (topicId - 1);
    }
}
