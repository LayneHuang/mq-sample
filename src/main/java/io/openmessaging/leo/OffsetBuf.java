package io.openmessaging.leo;

import java.nio.ByteBuffer;

public class OffsetBuf {

    public int offset;
    public ByteBuffer buf;

    public OffsetBuf(int offset, ByteBuffer buf) {
        this.offset = offset;
        this.buf = buf;
    }

}
