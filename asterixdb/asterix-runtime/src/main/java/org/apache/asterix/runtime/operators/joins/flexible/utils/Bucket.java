package org.apache.asterix.runtime.operators.joins.flexible.utils;

public class Bucket implements IBucket {
    final int bucketId;
    int startOffset;
    int endOffset;
    int side;
    int startFrame;
    int endFrame;
    public Bucket(int bucketId, int side, int startOffset, int endOffset, int startFrame, int endFrame) {
        this.bucketId = bucketId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.side = side;
        this.startFrame = startFrame;
        this.endFrame = endFrame;
    }
    @Override
    public int getBucketId() {
        return bucketId;
    }

    @Override
    public int getSide() {
        return side;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getStartFrame() {
        return startFrame;
    }

    public int getEndFrame() {
        return endFrame;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
