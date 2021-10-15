package io.openmessaging.wal;

import io.openmessaging.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Encoder extends Thread {
    public static final Logger log = LoggerFactory.getLogger(Encoder.class);
    public final BlockingQueue<WritePage> writeBq = new LinkedBlockingQueue<>(Constant.BQ_SIZE);
    private static final Map<Integer, Integer> APPEND_OFFSET_MAP = new ConcurrentHashMap<>();
    private final BlockingQueue<WalInfoBasic> logsBq;
    private final Map<Integer, Idx> IDX;
    private final int walId;
    private int waitCnt;

    public Encoder(int walId, BlockingQueue<WalInfoBasic> logsBq, Map<Integer, Idx> IDX) {
        this.walId = walId;
        this.logsBq = logsBq;
        this.IDX = IDX;
    }

    @Override
    public void run() {
        try {
            int emptyCnt = 0;
            while (true) {
                WalInfoBasic info = logsBq.poll(20, TimeUnit.MILLISECONDS);
//                long b = System.nanoTime();
                if (info == null && cur == 0) {
                    emptyCnt++;
                    if (emptyCnt > 100) {
                        break;
                    } else continue;
                }
                emptyCnt = 0;
                if (info == null && cur > 0) {
                    force();
                }
                if (info != null) {
                    submit(info);
                    if (waitCnt > 2 && cur >= Constant.FORCE_LIMIT) {
                        force();
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void submit(WalInfoBasic info) {
        byte[] bs = info.encodeToB();
        // wal 分段
        if (pos + info.getSize() >= Constant.WRITE_BEFORE_QUERY) {
            force();
            pos = 0;
            part++;
        }
        info.walPart = part;
        info.walPos = pos;
        // 获取偏移
        info.pOffset = APPEND_OFFSET_MAP.computeIfAbsent(info.getKey(), k -> -1) + 1;
        APPEND_OFFSET_MAP.put(info.getKey(), (int) info.pOffset);
        // 索引
        Idx idx = IDX.computeIfAbsent(info.getKey(), k -> new Idx());
        idx.add((int) info.pOffset, info.walId, info.walPart, info.walPos + WalInfoBasic.BYTES, info.valueSize);

        // 落盘
        put(bs);
    }

    private final byte[] tmp = new byte[Constant.WRITE_SIZE];

    private int cur = 0;

    private int pos = 0;

    private int part = 0;

    private int logCount = 0;

    private int forceCnt = 0;

    private int mergeCnt = 0;

    private void put(byte[] bs) {
        if (bs.length == 0) return;
        try {
            waitCnt++;
            logCount++;
            for (int i = 0; i < bs.length; ++i) {
                tmp[cur++] = bs[i];
                pos++;
                if (cur == Constant.WRITE_SIZE) {
                    mergeCnt++;
                    int fullCount = i == bs.length - 1 ? logCount : logCount - 1;
                    writeBq.put(new WritePage(fullCount, walId, part, pos, tmp, cur));
                    cur = 0;
                    waitCnt = 0;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void force() {
        if (cur <= 0) return;
        forceCnt++;
        if (forceCnt % 1000 == 0) log.info("ENCODER FORCE: {}, MERGE: {}", forceCnt, mergeCnt);
        try {
            writeBq.put(new WritePage(logCount, walId, part, pos, tmp, cur));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cur = 0;
        waitCnt = 0;
    }
}
