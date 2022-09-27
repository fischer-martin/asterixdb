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
    protected List<Integer> numberOfBucketsInEachFrame = new ArrayList<>();
    protected int currentOffsetInLastFrame = 0;
    protected int frameCapacity;
    protected int currentLargestFrameNumber = 0;
    // The byte size of total frames that are allocated to the headers and contents
    protected int currentByteSize = 0;
    protected int bucketCount = 0;
    protected TuplePointer tempTuplePointer = new TuplePointer();
    protected int tableSize;
    protected int frameSize;
    protected IHyracksFrameMgrContext ctx;

    protected int frameEntryCapacity;

    public SimpleSerializableBucketIdList(int tableSize, final IHyracksFrameMgrContext ctx, boolean req) throws HyracksDataException {
        this.ctx = ctx;
        frameSize = ctx.getInitialFrameSize();
        this.frameEntryCapacity = frameSize / (INT_SIZE * 5);
        if(req) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                throw new HyracksDataException("Can't initialize the Hash Table. Please assign more memory.");
            }

            IntSerDeBuffer frame = new IntSerDeBuffer(newFrame);

            contents.add(frame);
            numberOfBucketsInEachFrame.add(0);
            currentOffsetInLastFrame = 0;
        }
        this.tableSize = tableSize;


    }

    ByteBuffer getFrame(int size) throws HyracksDataException {
        currentByteSize += size;
        return ctx.allocateFrame(size);
    }



    @Override
    public int[] getEntry(int index) {

        int contentFrameIndex = index / this.frameEntryCapacity;
        if(contentFrameIndex < 0 || contentFrameIndex > currentLargestFrameNumber)
            return null;
        int offsetInContentFrame = (index % this.frameEntryCapacity) * 5;

        IntSerDeBuffer frame = contents.get(contentFrameIndex);

        return new int[] {
                frame.getInt(offsetInContentFrame),
                frame.getInt(offsetInContentFrame+1),frame.getInt(offsetInContentFrame+2),
                frame.getInt(offsetInContentFrame+3),frame.getInt(offsetInContentFrame+4)
        };
    }

    @Override
    public int getNumEntries() {
        return bucketCount;
    }

    public TuplePointer getBuildTuplePointer(int bucketId) {
        //TODO this function needs to traverse all entries and find the given bucket id
        int contentFrameIndex = 0;
        while(contentFrameIndex <= currentLargestFrameNumber) {
            int offsetInContentFrame = 0;
            IntSerDeBuffer frame = contents.get(contentFrameIndex);

            while(offsetInContentFrame < this.frameCapacity)  {
                if(frame.getInt(offsetInContentFrame) == bucketId) {
                    return new TuplePointer(frame.getInt(offsetInContentFrame+1), frame.getInt(offsetInContentFrame+2));
                }
                offsetInContentFrame += 5;
            }
            contentFrameIndex++;
        }

        return null;
    }

    public TuplePointer getProbeTuplePointer(int bucketId) {
        //TODO this function needs to traverse all entries and find the given bucket id
        int contentFrameIndex = 0;
        while(contentFrameIndex <= currentLargestFrameNumber) {
            int offsetInContentFrame = 0;
            IntSerDeBuffer frame = contents.get(contentFrameIndex);

            while(offsetInContentFrame < this.frameCapacity)  {
                if(frame.getInt(offsetInContentFrame) == bucketId) {
                    return new TuplePointer(frame.getInt(offsetInContentFrame+3), frame.getInt(offsetInContentFrame+4));
                }
                offsetInContentFrame += 5;
            }
            contentFrameIndex++;
        }

        return null;
    }



    @Override
    public void reset() {
        numberOfBucketsInEachFrame.clear();
        for (int i = 0; i < contents.size(); i++) {
            numberOfBucketsInEachFrame.add(0);
        }
        currentOffsetInLastFrame = 0;
        currentLargestFrameNumber = 0;
        bucketCount = 0;
        currentByteSize = 0;
    }


    @Override
    public void close() {
        int nFrames = contents.size();

        contents.clear();
        numberOfBucketsInEachFrame.clear();
        bucketCount = 0;
        currentByteSize = 0;
        currentLargestFrameNumber = 0;
        ctx.deallocateFrames(nFrames);
    }

    @Override
    public boolean insert(int bucketId, TuplePointer buildTuplePointer, TuplePointer probeTuplePointer ) throws HyracksDataException {
        int requiredIntCapacity = INT_SIZE * 5;
        IntSerDeBuffer contentFrame;

        if (currentOffsetInLastFrame + requiredIntCapacity > frameCapacity) {
            ByteBuffer newFrame = getFrame(frameSize);
            if (newFrame == null) {
                return false;
            }
            contentFrame = new IntSerDeBuffer(newFrame);
            currentLargestFrameNumber++;
            contents.add(contentFrame);
            numberOfBucketsInEachFrame.add(0);
            currentOffsetInLastFrame = 0;
        } else {
            contentFrame = contents.get(currentLargestFrameNumber);
        }
        //add the new bucket id
        contentFrame.writeInt(currentOffsetInLastFrame, bucketId);
        //add tuple pointer from build side
        contentFrame.writeInt(currentOffsetInLastFrame + 1, buildTuplePointer.getFrameIndex());
        contentFrame.writeInt(currentOffsetInLastFrame + 2, buildTuplePointer.getTupleIndex());
        //add tuple pointer from probe side
        contentFrame.writeInt(currentOffsetInLastFrame + 3, probeTuplePointer.getFrameIndex());
        contentFrame.writeInt(currentOffsetInLastFrame + 4, probeTuplePointer.getTupleIndex());

        numberOfBucketsInEachFrame.set(currentLargestFrameNumber, numberOfBucketsInEachFrame.get(currentLargestFrameNumber) + 1);
        currentOffsetInLastFrame += 5;
        bucketCount++;

        return true;
    }

    public boolean updateProbeBucket(int bucketId, TuplePointer probeTuplePointer) throws HyracksDataException {
        int contentFrameIndex = 0;
        while(contentFrameIndex <= currentLargestFrameNumber) {
            int offsetInContentFrame = 0;
            IntSerDeBuffer frame = contents.get(contentFrameIndex);

            while(offsetInContentFrame < this.frameCapacity)  {
                if(frame.getInt(offsetInContentFrame) == bucketId) {
                    //update tuple pointer from probe side
                    frame.writeInt(offsetInContentFrame + 3, probeTuplePointer.getFrameIndex());
                    frame.writeInt(offsetInContentFrame + 4, probeTuplePointer.getTupleIndex());

                    return true;
                }
                offsetInContentFrame += 5;
            }
            contentFrameIndex++;
        }

        return false;
    }

    public boolean updateBuildBucket(int bucketId, TuplePointer buildTuplePointer) throws HyracksDataException {
        int contentFrameIndex = 0;
        while(contentFrameIndex <= currentLargestFrameNumber) {
            int offsetInContentFrame = 0;
            IntSerDeBuffer frame = contents.get(contentFrameIndex);

            while(offsetInContentFrame < this.frameCapacity)  {
                if(frame.getInt(offsetInContentFrame) == bucketId) {
                    //update tuple pointer from probe side
                    frame.writeInt(offsetInContentFrame + 1, buildTuplePointer.getFrameIndex());
                    frame.writeInt(offsetInContentFrame + 2, buildTuplePointer.getTupleIndex());

                    return true;
                }
                offsetInContentFrame += 5;
            }
            contentFrameIndex++;
        }
        return false;
    }

    public void printInfo() {
        int contentFrameIndex = 0;
        int printedCounter = 0;
        StringBuilder dS = new StringBuilder(this.toString());
        dS.append("\nBucket Id\tBuild F\tBuild T\tProbe F\tProbe T\n");
        while(contentFrameIndex <= currentLargestFrameNumber) {
            int offsetInContentFrame = 0;
            IntSerDeBuffer frame = contents.get(contentFrameIndex);

            while(offsetInContentFrame < this.frameCapacity)  {
                dS.append(frame.getInt(offsetInContentFrame)).append("\t").append(frame.getInt(offsetInContentFrame + 1)).append("\t").append(frame.getInt(offsetInContentFrame + 2)).append("\t").append(frame.getInt(offsetInContentFrame + 3)).append("\t").append(frame.getInt(offsetInContentFrame + 4)).append("\n");
                offsetInContentFrame += 5;
                printedCounter++;
                if(printedCounter == bucketCount) break;
            }
            contentFrameIndex++;
        }
        System.out.println(dS);
    }

    public int lastBucket() {
        int i = bucketCount - 1;
        while(i >= 0) {
            int[] b = getEntry(i);
            if(b[1] > -1) return b[0];
            i--;
        }
        return -1;
    }

    public int secondLastBucket() {
        int i = bucketCount - 1;
        int j = 2;
        while(i >= 0) {
            int[] b = getEntry(i);
            if(b[1] > -1) {
                j--;
                if(j == 0)
                    return b[0];
            }
            i--;
        }
        return -1;
    }

    @Override
    public int size() {
        return bucketCount;
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
