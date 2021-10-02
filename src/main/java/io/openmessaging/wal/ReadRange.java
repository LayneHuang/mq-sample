package io.openmessaging.wal;

public class ReadRange {

    public long begin;

    public long end;

    public ReadRange(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }
}
