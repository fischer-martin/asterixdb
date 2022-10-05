package org.apache.asterix.runtime.operators.joins.flexible.utils;

import java.nio.ByteBuffer;

public interface IBucket {
    int getBucketId();
    int getSide();
    int getStartOffset();
    int getStartFrame();
    int getEndFrame();
    int getEndOffset();
}
