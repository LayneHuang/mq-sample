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

    private static final int BOX_SIZE = 32;

    public int topic;
    public int queueId;
    public List<MemoryBox> bufferList = new ArrayList<>();

    public static class MemoryBox{
        byte[] partitionIdArray = new byte[BOX_SIZE];
        byte[] logNumArray = new byte[BOX_SIZE];
        int[] positionArray = new int[BOX_SIZE];
        short[] dataSizeArray = new short[BOX_SIZE];
        byte index;

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
        bufferList.add(new MemoryBox());
    }

    public void writeIndex(byte partitionId, byte logNumAdder, int position, short dataSize) {
        MemoryBox memoryBox = bufferList.get(bufferList.size() - 1);
        if (memoryBox.index >= BOX_SIZE) {
            memoryBox = new MemoryBox();
            bufferList.add(memoryBox);
        }
        memoryBox.add(partitionId,logNumAdder,position,dataSize);
    }

    public Map<Integer, ByteBuffer> getRange(long offsetStart, int fetchNum) throws IOException {
        Map<Integer, ByteBuffer> map = new HashMap<>(fetchNum);
        for (long offset = offsetStart; offset < offsetStart + fetchNum; offset++) {
            int index = (int) (offset / 32);
            int indexPosition = (int) (offset % 32);
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
            map.put((int) (offset - offsetStart), dataBuf);
        }
        return map;
    }

}
