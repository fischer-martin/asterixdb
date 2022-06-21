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
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is an extension of SimpleSerializableHashTable class.
 * A buffer manager needs to be assigned to allocate/release frames for this table so that
 * the maximum memory usage can be bounded under the certain limit.
 */
public class SerializableHashTableForKeyPair extends SimpleSerializableHashTableForKeyPair {

    protected double garbageCollectionThreshold;
    protected int wastedIntSpaceCount = 0;
    protected ISimpleFrameBufferManager bufferManager;
    protected HashSet<Integer> keys = new HashSet<>();

    public SerializableHashTableForKeyPair(int tableSize, final IHyracksFrameMgrContext ctx,
                                           ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this(tableSize, ctx, bufferManager, 0.1);
    }

    public SerializableHashTableForKeyPair(int tableSize, final IHyracksFrameMgrContext ctx,
                                           ISimpleFrameBufferManager bufferManager, double garbageCollectionThreshold) throws HyracksDataException {
        super(tableSize, ctx, false);
        this.bufferManager = bufferManager;
        if (tableSize > 0) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                throw new HyracksDataException("Can't allocate a frame for Hash Table. Please allocate more budget.");
            }
            BoolSerDeBuffer frame = new BoolSerDeBuffer(newFrame);
            frameCapacity = frame.capacity();
            contents.add(frame);
            currentOffsetInEachFrameList.add(0);
        }
        this.garbageCollectionThreshold = garbageCollectionThreshold;
    }

    public HashSet<Integer> getKeys() {
        return keys;
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
    void increaseWastedSpaceCount(int size) {
        wastedIntSpaceCount += size;
    }

    @Override
    public void reset() {
        super.reset();
        currentByteSize = 0;
    }

    @Override
    public void close() {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                bufferManager.releaseFrame(headers[i].getByteBuffer());
                headers[i] = null;
            }
        }
        for (int i = 0; i < contents.size(); i++) {
            bufferManager.releaseFrame(contents.get(i).getByteBuffer());
        }
        contents.clear();
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
    }
}
