package org.apache.asterix.runtime.operators.joins.flexible.utils;

import java.nio.ByteBuffer;

public class Bucket implements IBucket {
    final int bucketId;
    public Bucket(int bucketId) {
        this.bucketId = bucketId;
    }
    @Override
    public boolean startReadingBucket() {
        return false;
    }

    @Override
    public boolean hasNextFrame() {
        return false;
    }

    @Override
    public ByteBuffer nextFrame() {
        return null;
    }
}
