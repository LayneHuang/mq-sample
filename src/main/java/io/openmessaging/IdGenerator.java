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

    public static int getId(String key) {
        if (ID_MAP.containsKey(key)) return ID_MAP.get(key);
        int result = Constant.hash(key);
        ID_MAP.put(key, result);
        return result;
    }

}
