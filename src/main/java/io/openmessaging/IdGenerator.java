package io.openmessaging;

import java.util.HashMap;

/**
 * IdGenerator
 *
 * @author 86188
 * @since 2021/9/23
 */
public class IdGenerator {

    private static final HashMap<String, Integer> ID_MAP = new HashMap();

    private static int cnt = 0;

    private static final Object lock = new Object();

    public static int getId(String key) {
        if (ID_MAP.containsKey(key)) return ID_MAP.get(key);
        int result = -1;
        synchronized (lock) {
            if (ID_MAP.containsKey(key)) return ID_MAP.get(key);
            result = cnt++;
            ID_MAP.put(key, result);
        }
        return result;
    }

}
