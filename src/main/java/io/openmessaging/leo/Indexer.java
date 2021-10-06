package io.openmessaging.leo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.INDEX_PATH;
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
        Path topicDir = INDEX_PATH.resolve(String.valueOf(topic));
        try {
            Files.createDirectories(topicDir);
            indexFile = topicDir.resolve(String.valueOf(queueId));
            Files.deleteIfExists(indexFile);
            Files.createFile(indexFile);
        } catch (IOException e) {
        }
    }

    public void writeIndex(ByteBuffer indexBuf) {
        try {
            if (tempBuf.remaining() < indexBuf.limit()) {
                tempBuf.flip();
                FileChannel fileChannel = FileChannel.open(
                        indexFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND
                );
                fileChannel.write(tempBuf);
                fileChannel.force(false);
                fileChannel.close();
                System.out.println("idx F " + topic + " , " + queueId);
                tempBuf = ByteBuffer.allocate(INDEX_TEMP_BUF_SIZE);
            }
            tempBuf.put(indexBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
