package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Partition extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Partition.class);

    private final int walId;

    public final BlockingQueue<Long> readBq = new LinkedBlockingQueue<>(Constant.LOG_SEGMENT_SIZE);

    public final BlockingQueue<PageForWrite> writeBq = new LinkedBlockingQueue<>(Constant.LOG_SEGMENT_SIZE);

    private final Page page = new Page();

    public Partition(int walId) {
        this.walId = walId;
    }

    @Override
    public void run() {
        try (FileChannel infoChannel = FileChannel.open(Constant.getWALInfoPath(walId), StandardOpenOption.READ)) {
            while (true) {
                long begin = readBq.take();
                if (begin == -1) break;
                // log.info("begin: {}", begin);
                MappedByteBuffer mappedBuffer = infoChannel.map(FileChannel.MapMode.READ_ONLY, begin, Constant.READ_BEFORE_QUERY);
                int pos = (int) begin;
                while (mappedBuffer.hasRemaining()) {
                    WalInfoBasic msgInfo = new WalInfoBasic();
                    msgInfo.decode(mappedBuffer);
//                    msgInfo.show();
                    msgInfo.infoPos = pos;
                    pos += Constant.MSG_SIZE;
                    ByteBuffer buffer = page.partition(msgInfo);
                    if (!buffer.hasRemaining()) {
                        PageForWrite pageForWrite = new PageForWrite(msgInfo.topicId, msgInfo.queueId, buffer);
                        writeBq.put(pageForWrite);
                        page.clear(msgInfo);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
