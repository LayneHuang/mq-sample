package io.openmessaging.leo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.*;

public class Indexer {

    public int topic;
    public int queueId;
    public Path indexFile;
    private ByteBuffer tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);

    public Indexer(int topic, int queueId) {
        this.topic = topic;
        this.queueId = queueId;
        Path topicDir = DIR_ESSD.resolve(String.valueOf(topic));
        try {
            Files.createDirectories(topicDir);
        } catch (IOException e) {
        }
        try {
            indexFile = topicDir.resolve(String.valueOf(queueId));
            Files.createFile(indexFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void writeIndex(ByteBuffer indexBuf) {
        try {
            if (tempBuf.remaining() < indexBuf.limit()) {
                tempBuf.flip();
                FileChannel fileChannel = FileChannel.open(
                        indexFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND
//                        , StandardOpenOption.DSYNC
                );
                fileChannel.write(tempBuf);
                fileChannel.force(false);
                fileChannel.close();
                tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
            }
            tempBuf.put(indexBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized ByteBuffer getTempBuf() {
        ByteBuffer clone = ByteBuffer.allocate(tempBuf.capacity());
        tempBuf.rewind();
        clone.put(tempBuf);
        clone.flip();
        return clone;
    }

}
