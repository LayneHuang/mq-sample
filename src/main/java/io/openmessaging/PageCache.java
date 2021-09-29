package io.openmessaging;

import io.openmessaging.wal.WalInfoBasic;

import java.nio.ByteBuffer;

public class PageCache {

    private static final PageCache ins = new PageCache();

    /**
     * msg size page
     */
    private final int[][][] msgSizePage
            = new int[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT][Constant.CACHE_LEN];

    private final long[][][] msgPosPage
            = new long[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT][Constant.CACHE_LEN];


    private final int[][] pLen
            = new int[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT];


    public static PageCache getIns() {
        return ins;
    }

    public void add(WalInfoBasic info) {
        int len = getLen(info.topicId, info.queueId);
        msgSizePage[info.topicId][info.queueId][len] = info.valueSize;
        msgPosPage[info.topicId][info.queueId][len] = info.valuePos;
        pLen[info.topicId][info.queueId]++;
    }

    public int getLen(int topicId, int queueId) {
        return pLen[topicId][queueId];
    }

    public boolean isFull(int topicId, int queueId) {
        return pLen[topicId][queueId] >= Constant.CACHE_LEN;
    }

    public void clear(int topicId, int queueId) {
        pLen[topicId][queueId] = 0;
    }

    public ByteBuffer encode(int topicId, int queueId, ByteBuffer buffer) {
        buffer.clear();
        for (int i = 0; i < Constant.CACHE_LEN; ++i) {
            buffer.putInt(msgSizePage[topicId][queueId][i]);
            buffer.putLong(msgPosPage[topicId][queueId][i]);
        }
        return buffer;
    }
}
