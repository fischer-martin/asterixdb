package org.apache.asterix.runtime.operators.joins.flexible.utils;

import java.nio.ByteBuffer;

public interface IBucket {
    int getBucketId();
    int getSide();
    long getSize();
}
