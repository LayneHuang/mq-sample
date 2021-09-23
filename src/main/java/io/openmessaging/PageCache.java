package io.openmessaging;

import io.openmessaging.wal.WalInfoBasic;

public class PageCache {

    private static PageCache ins = new PageCache();

    /**
     * msg size page
     */
    private static final int[][][] msgSizePage
            = new int[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT][Constant.CACHE_LEN];

    private static final long[][][] msgPosPage
            = new long[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT][Constant.CACHE_LEN];


    private static final int[][] pLen
            = new int[Constant.MAX_TOPIC_COUNT][Constant.MAX_QUEUE_COUNT];


    public static void add(WalInfoBasic info) {
        int len = getLen(info.topicId, info.queueId);
        msgSizePage[info.topicId][info.queueId][len] = info.size;
        msgPosPage[info.topicId][info.queueId][len] = info.pos;
        pLen[info.topicId][info.queueId]++;
    }

    public static int getLen(int topicId, int queueId) {
        return pLen[topicId][queueId];
    }

    public static boolean isFull(int topicId, int queueId) {
        return pLen[topicId][queueId] >= Constant.CACHE_LEN;
    }
}
