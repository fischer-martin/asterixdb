/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.flexible.utils;

public class Bucket {
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

    public int getBucketId() {
        return bucketId;
    }

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
