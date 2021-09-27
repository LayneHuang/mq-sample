package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.openmessaging.solve.FinkysMessageQueueImpl.DIR_ESSD;

public class FinkysPage {

    private Map<Integer, Long> globalIdOffsetMap = new HashMap<>();

    private String topic;

    private ByteBuffer byteBuffer = ByteBuffer.allocate(32 * 1024 * 1024);

    private List<Map<Integer, Long>> idOffsetMapList = new ArrayList<>();

    private Map<Integer, Long> currentIdOffsetMap = new HashMap<>();

    private FileChannel currentFileChannel;

    public FinkysPage(String topic) {
        this.topic = topic;
    }


    private Path getFilePath(int index) {
        return DIR_ESSD.resolve(topic + "_" + index + ".d");
    }

    public long append(int queueId, ByteBuffer data) throws IOException {
        if (currentFileChannel == null) {
            currentFileChannel = FileChannel.open(
                    getFilePath(idOffsetMapList.size()),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
            );
        }
        Long offset = globalIdOffsetMap.get(queueId);
        if (offset == null) {
            offset = 0L;
        } else {
            offset++;
        }
        globalIdOffsetMap.put(queueId, offset);
        int space = 6 + data.limit();
        if (byteBuffer.remaining() >= space) {
            currentIdOffsetMap.put(queueId, offset);
            byteBuffer.putShort((short) data.limit());
            byteBuffer.putInt(queueId);
            byteBuffer.put(data);
        } else {
            idOffsetMapList.add(currentIdOffsetMap);
            currentIdOffsetMap = new HashMap<>();
            currentFileChannel.write(byteBuffer);
            currentFileChannel.close();
            currentFileChannel = null;
            byteBuffer.clear();
        }
        return offset;
    }

    public Map<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) throws IOException {
        for (int i = 0; i < idOffsetMapList.size(); i++) {
            Map<Integer, Long> integerLongMap = idOffsetMapList.get(i);
            Long mapOffset = integerLongMap.get(queueId);
            if (mapOffset != null && mapOffset > offset) {
                return getRangeFromIndex(i - 1, queueId, offset, fetchNum);
            }
        }
        return getRangeFromIndex(idOffsetMapList.size() - 1, queueId, offset, fetchNum);
    }

    private Map<Integer, ByteBuffer> getRangeFromIndex(int index, int queueId, long offset, int fetchNum) throws IOException {
        Map<Integer, ByteBuffer> result = new HashMap<>();
        if (index < 0) {
            index = 0;
        }
        for (int i = index; i < idOffsetMapList.size(); i++) {
            Map<Integer, Long> integerLongMap = idOffsetMapList.get(i);
            Long mapOffset = integerLongMap.get(queueId);
            if (mapOffset != null && mapOffset <= offset + fetchNum - 1) {
                Path dataPath = getFilePath(i);
                if (Files.exists(dataPath)) {
                    FileChannel dataChannel = FileChannel.open(dataPath, StandardOpenOption.READ);
                    ByteBuffer headBuf = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
                    while (true) {
                        if (dataChannel.read(headBuf) <= 0) {
                            break;
                        }
                        headBuf.flip();
                        short curLen = headBuf.getShort();
                        int curQueueId = headBuf.getInt();
                        headBuf.flip();
                        if (queueId == curQueueId) {
                            if (mapOffset >= offset) {
                                if (mapOffset > offset + fetchNum - 1) {
                                    break;
                                }
                                ByteBuffer msgBuf = ByteBuffer.allocate(curLen);
                                dataChannel.read(msgBuf);
                                msgBuf.flip();
                                result.put(result.size(), msgBuf);
                            } else {
                                dataChannel.position(dataChannel.position() + curLen);
                            }
                            mapOffset++;
                        } else {
                            dataChannel.position(dataChannel.position() + curLen);
                        }

                    }
                    dataChannel.close();
                }
            }
        }
        return result;
    }
}
