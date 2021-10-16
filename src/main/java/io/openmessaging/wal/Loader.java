package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class Loader extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Loader.class);

    private final int walId;

    private final Map<Integer, Idx> IDX;

    public Loader(int walId, Map<Integer, Idx> IDX) {
        this.walId = walId;
        this.IDX = IDX;
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
        FileChannel channel = null;
        ByteBuffer buffer = ByteBuffer.allocate(Constant.WRITE_SIZE);
        WalInfoBasic info = new WalInfoBasic();
        while (Constant.getWALInfoPath(walId, part).toFile().exists()) {
            log.info("Read File: {}, {}", walId, part);
            if (channel != null) channel.close();
            channel = FileChannel.open(Constant.getWALInfoPath(walId, part), StandardOpenOption.READ);
            int walPos = 0;
            boolean isEnd = false;
            while (channel.read(buffer) > 0) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    info.decode(channel, buffer, true);
                    if (info.topicId == 0) {
                        isEnd = true;
                        break;
                    }
                    info.walId = walId;
                    info.walPart = part;
                    info.walPos = walPos;
                    // 索引
                    Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
                    idx.add((int) info.pOffset, info.walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
                    // 偏移
                    walPos += info.getSize();
                }
                buffer.clear();
                if (isEnd) break;
            }
            part++;
        }
        if (channel != null) {
            channel.close();
        }
    }

}
