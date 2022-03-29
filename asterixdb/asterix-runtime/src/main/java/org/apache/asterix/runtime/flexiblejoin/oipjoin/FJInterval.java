package org.apache.asterix.runtime.flexiblejoin.oipjoin;

public class FJInterval {
    public long start;
    public long end;
    public FJInterval(long start, long end) {
        this.start = start;
        this.end = end;
    }
}
