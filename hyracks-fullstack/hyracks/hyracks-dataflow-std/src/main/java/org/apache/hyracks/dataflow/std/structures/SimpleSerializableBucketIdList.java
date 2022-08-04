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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SimpleSerializableBucketIdList implements ISerializableBucketIdList {

    // unit size: int
    protected static final int INT_SIZE = 4;
    // Initial entry slot size
    protected static final int INIT_ENTRY_SIZE = 4;
    protected static final int INVALID_VALUE = 0xFFFFFFFF;
    protected static final byte INVALID_BYTE_VALUE = (byte) 0xFF;

    // Content frame list
    protected List<IntSerDeBuffer> contents = new ArrayList<>();
    protected List<Integer> currentOffsetInEachFrameList = new ArrayList<>();
    protected int frameCapacity;
    protected int currentLargestFrameNumber = 0;
    // The byte size of total frames that are allocated to the headers and contents
    protected int currentByteSize = 0;
    protected int tupleCount = 0;
    protected TuplePointer tempTuplePointer = new TuplePointer();
    protected int tableSize;
    protected int frameSize;
    protected IHyracksFrameMgrContext ctx;

    protected int frameEntryCapacity;

    public SimpleSerializableBucketIdList(int tableSize, final IHyracksFrameMgrContext ctx) {
        this.ctx = ctx;
        frameSize = ctx.getInitialFrameSize();
        int residual = tableSize * INT_SIZE * 3 % frameSize == 0 ? 0 : 1;
        int requiredFrames = tableSize * INT_SIZE * 3 / frameSize + residual;

        this.tableSize = tableSize;

        this.frameEntryCapacity = frameCapacity / 3;


    }

    ByteBuffer getFrame(int size) throws HyracksDataException {
        currentByteSize += size;
        return ctx.allocateFrame(size);
    }



    @Override
    public int getBucketId(int index) {

        int contentFrameIndex = index / this.frameEntryCapacity;
        if(contentFrameIndex < 0 || contentFrameIndex > currentLargestFrameNumber)
            return -1;
        int offsetInContentFrame = (index % this.frameEntryCapacity) * 3;

        IntSerDeBuffer frame = contents.get(contentFrameIndex);
        return frame.getInt(offsetInContentFrame);
    }

    @Override
    public TuplePointer getTuplePointer(int index) {

        int contentFrameIndex = index / this.frameEntryCapacity;
        if(contentFrameIndex < 0 || contentFrameIndex > currentLargestFrameNumber)
            return new TuplePointer();
        int offsetInContentFrame = (index % this.frameEntryCapacity) * 3;

        IntSerDeBuffer frame = contents.get(contentFrameIndex);
        return new TuplePointer(frame.getInt(offsetInContentFrame+1), frame.getInt(offsetInContentFrame+2));
    }

    @Override
    public void reset() {


        currentOffsetInEachFrameList.clear();
        for (int i = 0; i < contents.size(); i++) {
            currentOffsetInEachFrameList.add(0);
        }

        currentLargestFrameNumber = 0;
        tupleCount = 0;
        currentByteSize = 0;
    }


    @Override
    public void close() {
        int nFrames = contents.size();

        contents.clear();
        currentOffsetInEachFrameList.clear();
        tupleCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
        ctx.deallocateFrames(nFrames);
    }

    public boolean insert(int bucketId, TuplePointer tuplePointer) throws HyracksDataException {
        int lastOffsetInCurrentFrame = currentOffsetInEachFrameList.get(currentLargestFrameNumber);
        int requiredIntCapacity = INT_SIZE * 4;
        IntSerDeBuffer contentFrame;

        if (lastOffsetInCurrentFrame + requiredIntCapacity > frameCapacity) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                return false;
            }
            contentFrame = new IntSerDeBuffer(newFrame);
            currentLargestFrameNumber++;
            contents.add(contentFrame);
            currentOffsetInEachFrameList.add(0);
            lastOffsetInCurrentFrame = 0;
        } else {
            contentFrame = contents.get(currentLargestFrameNumber);

        }
        contentFrame.writeInt(lastOffsetInCurrentFrame, bucketId);
        contentFrame.writeInt(lastOffsetInCurrentFrame + 1, tuplePointer.getFrameIndex());
        contentFrame.writeInt(lastOffsetInCurrentFrame + 2, tuplePointer.getTupleIndex());

        tupleCount++;

        return true;
    }

    @Override
    public int size() {
        return tupleCount;
    }

    protected int getHeaderFrameIndex(int entry) {
        int frameIndex = (entry % tableSize) * 2 / frameCapacity;
        return frameIndex;
    }

    protected int getHeaderFrameOffset(int entry) {
        int offset = (entry % tableSize) * 2 % frameCapacity;
        return offset;
    }

    public static int getUnitSize() {
        return INT_SIZE;
    }


    static class IntSerDeBuffer {

        ByteBuffer byteBuffer;
        byte[] bytes;

        public IntSerDeBuffer(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            this.bytes = byteBuffer.array();
            resetFrame();
        }

        public int getInt(int pos) {
            int offset = pos * 4;
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16)
                    + ((bytes[offset + 2] & 0xff) << 8) + (bytes[offset + 3] & 0xff);
        }

        public void writeInt(int pos, int value) {
            int offset = pos * 4;
            bytes[offset++] = (byte) (value >> 24);
            bytes[offset++] = (byte) (value >> 16);
            bytes[offset++] = (byte) (value >> 8);
            bytes[offset] = (byte) (value);
        }

        public void writeInvalidVal(int intPos, int intRange) {
            int offset = intPos * 4;
            Arrays.fill(bytes, offset, offset + INT_SIZE * intRange, INVALID_BYTE_VALUE);
        }

        public int capacity() {
            return bytes.length / 4;
        }

        public int getByteCapacity() {
            return bytes.length;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public void resetFrame() {
            Arrays.fill(bytes, INVALID_BYTE_VALUE);
        }

    }

}
