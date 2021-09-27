package io.openmessaging.leo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.*;

public class DataPartition {

    public byte id;
    public Path logDir;
    public byte logNumAdder = -1;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;
    public long forcedPosition;

    public void init(byte id) {
        this.id = id;
        logDir = LOGS_PATH.resolve(String.valueOf(this.id));
        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
        }
    }

    public void openLog() throws IOException {
        logNumAdder++;
        Path logFile = logDir.resolve(String.valueOf(logNumAdder));
        Files.createFile(logFile);
        logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1_073_741_824);// 1G
    }

    private Path lastIndexPath;
    private FileChannel lastIndexFileChannel;

    public boolean writeLog(int topic, int queueId, long offset, ByteBuffer data, Indexer indexer) {
        ByteBuffer indexBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
        short msgLen = (short) data.limit();
        short dataSize = (short) (18 + msgLen);
        try {
            if (logMappedBuf.remaining() < dataSize) {
                unmap(logMappedBuf);
                logFileChannel.close();
                openLog();
            }
            int position = logMappedBuf.position();
            logMappedBuf.putInt(topic); // 4
            logMappedBuf.putInt(queueId); // 4
            logMappedBuf.putLong(offset); // 8
            logMappedBuf.putShort(msgLen); // 2
            logMappedBuf.put(data);
            logMappedBuf.force();
            // index
            indexBuf.put(id);
            indexBuf.put(logNumAdder);
            indexBuf.putInt(position);
            indexBuf.putShort(dataSize);
            indexBuf.flip();
            // index
            Path topicPath = DIR_ESSD.resolve(String.valueOf(topic));
            Path queueFile = topicPath.resolve(String.valueOf(queueId));
            try {
                Files.createDirectories(topicPath);
            } catch (IOException e) {
            }
            if (!queueFile.equals(lastIndexPath)) {
                if (lastIndexFileChannel != null) {
                    lastIndexFileChannel.close();
                }
                lastIndexPath = queueFile;
                lastIndexFileChannel = FileChannel.open(
                        lastIndexPath,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
//                            , StandardOpenOption.DSYNC
                );
            }
            lastIndexFileChannel.write(indexBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        indexer.writeIndex(indexBuf);
        return false;
    }

    public void indexForced() {

    }

}
