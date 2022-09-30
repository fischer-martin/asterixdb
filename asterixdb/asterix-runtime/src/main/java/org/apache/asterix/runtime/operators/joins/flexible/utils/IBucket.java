package org.apache.asterix.runtime.operators.joins.flexible.utils;

import java.nio.ByteBuffer;

public interface IBucket {
    boolean startReadingBucket();
    boolean hasNextFrame();
    ByteBuffer nextFrame();
}
