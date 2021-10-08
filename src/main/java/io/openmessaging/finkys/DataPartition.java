package io.openmessaging.finkys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.openmessaging.leo.DataManager.LOGS_PATH;
import static io.openmessaging.leo.DataManager.unmap;

public class DataPartition {

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;

    public FileChannel indexPosFileChannel;
    public Path indexPosFile;
    public ByteBuffer indexPosBuf = ByteBuffer.allocate(5);
    private int count = 1;

    public DataPartition(byte id) {
        this.id = id;
        logDir = LOGS_PATH.resolve(String.valueOf(this.id));
        try {
            Files.createDirectories(logDir);
            setupLog();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openLog() throws IOException {
        logNumAdder++;
        setupLog();
    }


    private void setupLog() throws IOException {
        Path logFile = logDir.resolve(String.valueOf(logNumAdder));
        Files.createFile(logFile);
        logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);// 1G
    }

    public void writeLog(int topic, int queueId, long offset, ByteBuffer data, MemoryIndexer indexer) {
        short msgLen = (short) data.limit();
        short dataSize = (short) (18 + msgLen);
        synchronized (indexer.LOCKER) {
            try {
                if (logMappedBuf.remaining() < dataSize) {
                    logMappedBuf.force();
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
                if (count++ % 2 == 0){
                    logMappedBuf.force();
                }
                // index
                indexer.writeIndex(id,logNumAdder,position,dataSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
