package io.openmessaging.leo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.leo2.DataManager.INDEX_BUF_SIZE;

public class Indexer {

    public byte topic;
    public short queueId;
    public List<OffsetBuf> fullBufs = new ArrayList<>();

    public Indexer(byte topic, short queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    // 相同topic+queue的数据不会被多个线程发送
    public void writeIndex(byte id, byte logNumAdder, int position, short dataSize) {
        ByteBuffer tempBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
        tempBuf.put(id);
        tempBuf.put(logNumAdder);
        tempBuf.putInt(position);
        tempBuf.putShort(dataSize);
        tempBuf.flip();
        fullBufs.add(new OffsetBuf(0, tempBuf));
    }

    public void writeIndex(OffsetBuf buf) {
        fullBufs.add(buf);
    }

}
