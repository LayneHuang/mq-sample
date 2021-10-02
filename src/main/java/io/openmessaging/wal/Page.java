package io.openmessaging.wal;

import io.openmessaging.Constant;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * PartitionResult
 *
 * @author 86188
 * @since 2021/9/30
 */
public class Page {

    public final Map<String, ByteBuffer> data = new HashMap<>();

    public boolean forceUpdate;

    /**
     * wal 分区
     */
    public ByteBuffer partition(WalInfoBasic infoBasic) {
        ByteBuffer buffer = data.computeIfAbsent(
                infoBasic.getKey(),
                k -> ByteBuffer.allocate(Constant.WRITE_BEFORE_QUERY)
        );
        return infoBasic.encodeSimple(buffer);
    }

    public void clear(WalInfoBasic infoBasic) {
        data.remove(infoBasic.getKey());
    }
}
