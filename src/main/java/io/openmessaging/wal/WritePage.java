package io.openmessaging.wal;

/**
 * WritePage
 *
 * @author laynehuang
 * @since 2021/10/9
 */
public class WritePage {
    public int walId;
    public int part;
    public int pos;
    public byte[] value;
    public int logCount;

    public WritePage(int logCount, int walId, int part, int pos, byte[] value, int valueSize) {
        this.logCount = logCount;
        this.walId = walId;
        this.part = part;
        this.pos = pos;
        this.value = new byte[valueSize];
        System.arraycopy(value, 0, this.value, 0, valueSize);
    }

    public WritePage(int logCount, int part, int pos, byte[] value, int valueSize) {
        this.logCount = logCount;
        this.part = part;
        this.pos = pos;
        this.value = new byte[valueSize];
        System.arraycopy(value, 0, this.value, 0, valueSize);
    }
}
