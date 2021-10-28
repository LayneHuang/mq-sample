package io.openmessaging.wal;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static io.openmessaging.leo2.Utils.UNSAFE;

/**
 * WalInfoBasic
 *
 * @author 86188
 * @since 2021/9/23
 */
public class WalInfoBasic {

    public int topicId;

    public int queueId;

    public long pOffset;

    public int valueSize;

    public int walId;

    public int walPart;

    public int walPos;

    public ByteBuffer value;

    public boolean isPmem;

    public WalInfoBasic() {
    }

    public WalInfoBasic(int walId, int walPart) {
        this.walId = walId;
        this.walPart = walPart;
    }

    public WalInfoBasic(int topicId, int queueId, long pOffset, int walId, int walPart, int walPos, int valueSize) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.pOffset = pOffset;
        this.walId = walId;
        this.walPart = walPart;
        this.walPos = walPos;
        this.valueSize = valueSize;
    }

    public WalInfoBasic(int pOffset, int walId, int walPart, int walPos, int valueSize, boolean isPmem) {
        this.pOffset = pOffset;
        this.walId = walId;
        this.walPart = walPart;
        this.walPos = walPos;
        this.valueSize = valueSize;
        this.isPmem = isPmem;
    }

    public WalInfoBasic(int walId, int topicId, int queueId, ByteBuffer value) {
        this.walId = walId;
        this.topicId = topicId;
        this.queueId = queueId;
        this.valueSize = value.limit();
        this.value = value;
    }

    public static final int BYTES = 7;

    public void encode(MappedByteBuffer buffer) {
        int position = buffer.position();
        long address = ((DirectBuffer) buffer).address() + position;
        // topicId
        UNSAFE.putByte(address, (byte) topicId);
        // queueId
        address++;
        UNSAFE.putByte(address, (byte) ((queueId >> 8) & 0xff));
        address++;
        UNSAFE.putByte(address, (byte) (queueId & 0xff));
        // pOffset
        address++;
        UNSAFE.putByte(address, (byte) ((pOffset >> 8) & 0xff));
        address++;
        UNSAFE.putByte(address, (byte) (pOffset & 0xff));
        // value
        address++;
        UNSAFE.putByte(address, (byte) ((valueSize >> 8) & 0xff));
        address++;
        UNSAFE.putByte(address, (byte) (valueSize & 0xff));
        // value
        address++;
        UNSAFE.copyMemory(value.array(), 16, null, address, value.limit());
        buffer.position(position + getSize());
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
        return getKey(topicId, queueId);
    }

    // 两个值都是 int 范围, 合并成 long
    public long getLongKey() {
        return (pOffset & Integer.MAX_VALUE) << 32 | getKey();
    }

    public String getStrKey() {
        return getKey() + "_" + pOffset;
    }
}
