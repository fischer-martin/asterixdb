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
package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;

import java.nio.ByteBuffer;

public class SerializableBucketIdList extends SimpleSerializableBucketIdList {

    protected double garbageCollectionThreshold;
    protected int wastedIntSpaceCount = 0;
    protected ISimpleFrameBufferManager bufferManager;

    public SerializableBucketIdList(int tableSize, final IHyracksFrameMgrContext ctx,
                                    ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        super(tableSize, ctx, false);
        this.bufferManager = bufferManager;
        if (tableSize > 0) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                throw new HyracksDataException("Can't allocate a frame for Hash Table. Please allocate more budget.");
            }
            IntSerDeBuffer frame = new IntSerDeBuffer(newFrame);
            frameCapacity = frame.capacity();
            contents.add(frame);
            numberOfBucketsInEachFrame.add(0);
        }
    }

    @Override
    ByteBuffer getFrame(int size) throws HyracksDataException {
        ByteBuffer newFrame = bufferManager.acquireFrame(size);
        if (newFrame != null) {
            currentByteSize += size;
        }
        return newFrame;
    }


    @Override
    public void reset() {
        super.reset();
        currentByteSize = 0;
    }

    @Override
    public void close() {
        for (int i = 0; i < contents.size(); i++) {
            bufferManager.releaseFrame(contents.get(i).getByteBuffer());
        }
        contents.clear();
        bucketCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
    }
}
