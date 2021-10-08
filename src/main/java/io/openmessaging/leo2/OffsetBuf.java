package io.openmessaging.leo2;

import java.nio.ByteBuffer;

public class OffsetBuf {

    public int offset;
    public ByteBuffer buf;

    public OffsetBuf(int offset, ByteBuffer buf) {
        this.offset = offset;
        this.buf = buf;
    }

}
