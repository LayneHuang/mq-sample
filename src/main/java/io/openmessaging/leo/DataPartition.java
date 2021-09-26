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

    public Short id;
    public Path logDir;
    public short logNumAdder = 0;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;

    public void init(short id) {
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

    public void writeLog(String topic, int queueId, ByteBuffer data) {
        try {
            if (logMappedBuf.remaining() < data.limit()) {
                unmap(logMappedBuf);
                logFileChannel.close();
                openLog();
            }
            int position = logMappedBuf.position();
            logMappedBuf.put(data);
            logMappedBuf.force();
            ByteBuffer indexBuf = ByteBuffer.allocate(INDEX_BUF_SIZE);
            indexBuf.putShort(id);
            indexBuf.putShort(logNumAdder);
            indexBuf.putInt(position);
            indexBuf.putShort((short) data.limit());
            indexBuf.flip();
            // index
            Path topicPath = DIR_ESSD.resolve(topic);
            Path queuePath = topicPath.resolve(String.valueOf(queueId));
            if (lastIndexPath == null) {
                try {
                    Files.createDirectories(topicPath);
                } catch (IOException e) {
                }
            }
            if (!queuePath.equals(lastIndexPath)) {
                if (lastIndexFileChannel != null) {
                    lastIndexFileChannel.close();
                }
                lastIndexPath = queuePath;
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
    }

}
