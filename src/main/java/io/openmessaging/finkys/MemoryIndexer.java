package io.openmessaging.finkys;

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

    private static final int BOX_SIZE = 32;

    public int topic;
    public int queueId;
    public List<MemoryBox> bufferList = new ArrayList<>();
    public MemoryBox currentBox;
    public final Object LOCKER = new Object();

    public static class MemoryBox{
        byte[] partitionIdArray = new byte[BOX_SIZE];
        byte[] logNumArray = new byte[BOX_SIZE];
        int[] positionArray = new int[BOX_SIZE];
        short[] dataSizeArray = new short[BOX_SIZE];
        byte index = 0;

        public void add(byte partitionId, byte logNumAdder, int position, short dataSize){
            this.partitionIdArray[index] = partitionId;
            this.logNumArray[index] = logNumAdder;
            this.positionArray[index] = position;
            this.dataSizeArray[index] = dataSize;
            index++;
        }
    }

    public MemoryIndexer(int topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
        currentBox = new MemoryBox();
        bufferList.add(currentBox);
    }

    public void writeIndex(byte partitionId, byte logNumAdder, int position, short dataSize) {
        if (currentBox.index >= BOX_SIZE) {
            currentBox = new MemoryBox();
            bufferList.add(currentBox);
        }
        currentBox.add(partitionId,logNumAdder,position,dataSize);
    }

    public Map<Integer, ByteBuffer> getRange(long offsetStart, int fetchNum) throws IOException {
        Map<Integer, ByteBuffer> map = new HashMap<>(fetchNum);
        for (long offset = offsetStart; offset < offsetStart + fetchNum; offset++) {
            int index = (int) (offset / BOX_SIZE);
            int indexPosition = (int) (offset % BOX_SIZE);
            if (index >= bufferList.size()){
                break;
            }
            MemoryBox memoryBox = bufferList.get(index);
            if (indexPosition >= memoryBox.index){
                break;
            }
            byte partitionId = memoryBox.partitionIdArray[indexPosition];
            byte logNum = memoryBox.logNumArray[indexPosition];
            int position = memoryBox.positionArray[indexPosition];
            short dataSize = memoryBox.dataSizeArray[indexPosition];
            Path logFile = LOGS_PATH.resolve(String.valueOf(partitionId)).resolve(String.valueOf(logNum));
            FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.READ);


            ByteBuffer dataBuf = ByteBuffer.allocate(dataSize);
            logChannel.read(dataBuf, position);
            logChannel.close();
            dataBuf.flip();

            dataBuf.getInt();
            dataBuf.getInt();
            offset = dataBuf.getLong();
            short msgLen = dataBuf.getShort();
            ByteBuffer msgBuf = ByteBuffer.allocate(msgLen);
            msgBuf.put(dataBuf);
            msgBuf.flip();
            map.put((int) (offset - offsetStart), msgBuf);
        }
        return map;
    }

}
