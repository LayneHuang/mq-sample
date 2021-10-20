package io.openmessaging.leo2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.leo2.DataManager.G1;

public class Cache {

    public static final Path DIR_PMEM = Paths.get("/pmem");
    public static final int DIR_PMEM_SIZE = 60;

    public static volatile AtomicInteger size = new AtomicInteger();

    public byte id;
    public Path logDir;
    public byte logNumAdder = Byte.MIN_VALUE;
    public FileChannel logFileChannel;
    public MappedByteBuffer logMappedBuf;
    private volatile boolean full = false;

    public Cache(byte id) {
        this.id = id;
        logDir = DIR_PMEM.resolve(String.valueOf(this.id));
        try {
            Files.createDirectories(logDir);
            setupLog();
            System.out.println(logDir.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void openLog(byte logNumAdder) throws IOException {
        if (size.get() < DIR_PMEM_SIZE) {
            this.logNumAdder = logNumAdder;
            setupLog();
            size.getAndIncrement();
        } else {
            full = true;
        }
    }

    private void setupLog() throws IOException {
        Path logFile = logDir.resolve(String.valueOf(logNumAdder));
        Files.createFile(logFile);
        logFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logMappedBuf = logFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, G1);// 1G
    }

    public void write(byte topic, short queueId, int offset, short msgLen, ByteBuffer data) {
        if (full) return;
        MappedByteBuffer tempBuf = logMappedBuf;
        tempBuf.put(topic); // 1
        tempBuf.putShort(queueId); // 2
        tempBuf.putInt(offset); // 4
        tempBuf.putShort(msgLen); // 2
        tempBuf.put(data);
    }

}
