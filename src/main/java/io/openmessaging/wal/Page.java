package io.openmessaging.wal;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * PartitionResult
 *
 * @author 86188
 * @since 2021/9/30
 */
public class Page {

    public final Map<String, List<String>> data = new HashMap<>();

    public boolean forceUpdate;

    /**
     * wal 分区
     */
    public void partition(WalInfoBasic infoBasic) {
        List<String> list = data.computeIfAbsent(infoBasic.getKey(), k -> new LinkedList<>());
        list.add(infoBasic.encodeSimple());
    }

    /**
     * 两个分区结果合并
     */
    public void union(Page part) {
        part.data.forEach((key, list1) -> {
            List<String> list = data.computeIfAbsent(key, k -> new LinkedList<>());
            list.addAll(list1);
        });
    }

    public void clear() {
        data.clear();
    }
}
