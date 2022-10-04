package org.apache.asterix.runtime.operators.joins.flexible.utils;

import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;

import java.nio.ByteBuffer;

public class Bucket implements IBucket {
    final int bucketId;
    long startOffset;
    long size;
    int side;
    public Bucket(int bucketId, long startOffset, long size, int side) {
        this.bucketId = bucketId;
        this.startOffset = startOffset;
        this.size = size;
        this.side = side;
    }
    @Override
    public int getBucketId() {
        return bucketId;
    }

    @Override
    public int getSide() {
        return side;
    }

    public long getSize() {
        return size;
    }

    public long getStartOffset() {
        return startOffset;
    }
}
