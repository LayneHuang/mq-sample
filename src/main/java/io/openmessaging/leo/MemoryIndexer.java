package io.openmessaging.leo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.openmessaging.leo.DataManager.LOGS_PATH;

public class MemoryIndexer {

    public int topic;
    public int queueId;
    public List<ByteBuffer> bufferList = new ArrayList<>();
    public final Object LOCKER = new Object();

    public MemoryIndexer(int topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
        bufferList.add(ByteBuffer.allocate(8 * 32));
    }

    public void writeIndex(byte partitionId, byte logNumAdder, int position, short dataSize) {
        ByteBuffer indexBuffer = bufferList.get(bufferList.size() - 1);
        if (!indexBuffer.hasRemaining()) {
            indexBuffer = ByteBuffer.allocate(8 * 32);
            bufferList.add(indexBuffer);
        }
        indexBuffer.put(partitionId);
        indexBuffer.put(logNumAdder);
        indexBuffer.putInt(position);
        indexBuffer.putShort(dataSize);
    }

    public Map<Integer, ByteBuffer> getRange(long offsetStart, int fetchNum) throws IOException {
        Map<Integer, ByteBuffer> map = new HashMap<>(fetchNum);
        for (long offset = offsetStart; offset < offsetStart + fetchNum; offset++) {
            int index = (int) (offset / 32);
            int indexPosition = (int) (offset % 32);
            if (index >= bufferList.size()){
                break;
            }
            ByteBuffer indexBuf = bufferList.get(index);
            if (index == bufferList.size() - 1) {
                synchronized (LOCKER) {
                    ByteBuffer clone = ByteBuffer.allocate(indexBuf.position());
                    indexBuf.rewind();
                    while (clone.hasRemaining()) {
                        clone.put(indexBuf.get());
                    }
                    clone.flip();
                    indexBuf = clone;
                }
            }
            if (indexPosition * 8 >= indexBuf.limit()){
                break;
            }
            indexBuf.position(indexPosition * 8);
            byte partitionId = indexBuf.get();
            byte logNum = indexBuf.get();
            int position = indexBuf.getInt();
            short dataSize = indexBuf.getShort();
            Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
            FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);


            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            logChannel.read(dataBuf, position);
            logChannel.close();
            dataBuf.flip();
            map.put((int) (offset - offsetStart), dataBuf);
        }
        return map;
    }

}
