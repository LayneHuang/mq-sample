package io.openmessaging.finkys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.DIR_ESSD;
import static io.openmessaging.leo.DataManager.INDEX_TEMP_BUF_SIZE;

public class Indexer {

    public int topic;
    public int queueId;
    public Path indexFile;
    private ByteBuffer tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
    public final Object LOCKER = new Object();

    public Indexer(int topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public boolean writeIndex(ByteBuffer indexBuf) {
        boolean force = false;
        try {
            if (tempBuf.remaining() < indexBuf.limit()) {
                tempBuf.flip();
                if (indexFile == null){
                    Path topicDir = DIR_ESSD.resolve(String.valueOf(topic));
                    try {
                        Files.createDirectories(topicDir);
                        indexFile = topicDir.resolve(String.valueOf(queueId));
                        Files.createFile(indexFile);
                    } catch (IOException e) {
                    }
                }
                FileChannel fileChannel = FileChannel.open(
                        indexFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                );
                fileChannel.write(tempBuf);
                fileChannel.force(false);
                fileChannel.close();
//                int partitionId = indexBuf.get() * INDEX_POS_SIZE;
//                byte logNumAdder = indexBuf.get();
//                int position = indexBuf.getInt();
//                INDEXER_POS_BUF.put(partitionId, logNumAdder);
//                INDEXER_POS_BUF.putInt(partitionId + 1, position);
//                INDEXER_POS_BUF.force();
//                indexBuf.rewind();
                tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
                force = true;
            }
            tempBuf.put(indexBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return force;
    }

    public ByteBuffer getTempBuf() {
        synchronized (LOCKER) {
            ByteBuffer clone = ByteBuffer.allocate(tempBuf.position());
            tempBuf.rewind();
            while (clone.hasRemaining()) {
                clone.put(tempBuf.get());
            }
            clone.flip();
            return clone;
        }
    }

}