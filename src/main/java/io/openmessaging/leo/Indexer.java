package io.openmessaging.leo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.DIR_ESSD;
import static io.openmessaging.leo.DataManager.INDEX_BUF_SIZE;

public class Indexer {

    public int topic;
    public int queueId;
    public Path indexFile;
    public ByteBuffer tempBuf = ByteBuffer.allocate(2048 * INDEX_BUF_SIZE);

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
                        , StandardOpenOption.DSYNC
                );
                fileChannel.write(tempBuf);
                fileChannel.close();
                tempBuf = ByteBuffer.allocate(2048 * INDEX_BUF_SIZE);
            }
            tempBuf.put(indexBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
