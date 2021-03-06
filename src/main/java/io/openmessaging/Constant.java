package io.openmessaging;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Constant
 *
 * @author 86188
 * @since 2021/9/17
 */
public class Constant {

    public static final Path DIR_ESSD = Paths.get("/essd");

    public static final int WAL_FILE_COUNT = 4;

    public static final int DEFAULT_MAX_THREAD_PER_WAL = 10;

    public static final int VALUE_POS_DIS = 29;

    public static final int WRITE_BEFORE_QUERY = (1 << VALUE_POS_DIS);

    public static final int KB = 1024;

    public static final int MB = 1024 * KB;

    public static final int GB = 1024 * MB;

    public static Path getWALInfoPath(int walId, int part) {
        return DIR_ESSD.resolve("WAL-INFO-" + walId + "-" + part + ".md");
    }

    public static Path getMetaPath() {
        return DIR_ESSD.resolve("META.md");
    }
}
