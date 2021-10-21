package io.openmessaging.wal;

import io.openmessaging.Constant;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Loader {
    private final Map<Integer, Idx> IDX;

    private final Map<Integer, AtomicInteger> APPEND_OFFSET_MAP;

    public Loader(Map<Integer, Idx> IDX, Map<Integer, AtomicInteger> APPEND_OFFSET_MAP) {
        this.IDX = IDX;
        this.APPEND_OFFSET_MAP = APPEND_OFFSET_MAP;
    }

    private final ExecutorService executor = new ThreadPoolExecutor(5, 10,
            300, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024)
    );

    public void run() {
        List<WalInfoBasic> files = new ArrayList<>();
        for (int walId = 0; walId < Constant.WAL_FILE_COUNT; ++walId) {
            for (int part = 1; ; ++part) {
                if (!Constant.getWALInfoPath(walId, part).toFile().exists()) break;
                files.add(new WalInfoBasic(walId, part));
            }
        }
        CountDownLatch latch = new CountDownLatch(files.size());
        files.forEach(file -> executor.submit(() -> {
            try {
                read(file.walId, file.walPart);
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        try {
            latch.await();
            executor.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void read(int walId, int part) throws IOException {
        WalInfoBasic info = new WalInfoBasic();
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
            APPEND_OFFSET_MAP.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
            // 索引
            Idx idx = IDX.computeIfAbsent(key, k -> new Idx());
            idx.add((int) info.pOffset, walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);
            // 偏移
            walPos += info.getSize();
        }
        // clean
        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
        channel.close();
    }
}
