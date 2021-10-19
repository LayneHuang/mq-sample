package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Loader extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Loader.class);

    private final int walId;

    private final Map<Integer, Idx> IDX;

    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP;

    public Loader(int walId, Map<Integer, Idx> IDX, Map<Integer, AtomicInteger> APPEND_OFFSET_MAP) {
        this.walId = walId;
        this.IDX = IDX;
        this.APPEND_OFFSET_MAP = APPEND_OFFSET_MAP;
    }

    @Override
    public void run() {
        try {
            read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void read() throws IOException {
        int part = 0;
        WalInfoBasic info = new WalInfoBasic();
        while (Constant.getWALInfoPath(walId, part).toFile().exists()) {
            log.info("Read File: {}, {}", walId, part);
            FileChannel channel = FileChannel.open(Constant.getWALInfoPath(walId, part), StandardOpenOption.READ);
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Constant.WRITE_BEFORE_QUERY);
            int walPos = 0;
            while (buffer.hasRemaining()) {
                info.decode(buffer, true);
                if (info.topicId == 0) {
                    break;
                }
                info.walId = walId;
                info.walPart = part;
                info.walPos = walPos;
                int key = info.getKey();
                // 块偏移
                info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
                // 索引
                Idx idx = IDX.computeIfAbsent(key, k -> new Idx());
                idx.add((int) info.pOffset, info.walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
                // 偏移
                walPos += info.getSize();
            }
            part++;
            // clean
            Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            }
            channel.close();
        }
    }
}
