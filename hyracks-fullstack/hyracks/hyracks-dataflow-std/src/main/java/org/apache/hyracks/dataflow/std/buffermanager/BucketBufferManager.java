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

package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FixedSizeFrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BucketBufferManager implements IBucketBufferManager {
    public static final IPartitionedMemoryConstrain NO_CONSTRAIN = new IPartitionedMemoryConstrain() {
        @Override
        public int frameLimit(int partitionId) {
            return Integer.MAX_VALUE;
        }
    };

    private IDeallocatableFramePool framePool;
    private IFrameBufferManager bufferManager;
    private int numTuples;
    private final FixedSizeFrame appendFrame;
    private final FixedSizeFrameTupleAppender appender;
    private BufferInfo tempInfo;
    private IPartitionedMemoryConstrain constrain;
    private FrameTupleAccessor frameTupleAccessor;

    // In case where a frame pool is shared by one or more buffer manager(s), it can be provided from the caller.
    public BucketBufferManager(IDeallocatableFramePool framePool, FrameTupleAccessor frameTupleAccessor) throws HyracksDataException {
        this.framePool = framePool;
        this.bufferManager = new FrameBufferManager();
        this.appendFrame = new FixedSizeFrame();
        this.appender = new FixedSizeFrameTupleAppender();
        this.tempInfo = new BufferInfo(null, -1, -1);
        this.frameTupleAccessor = frameTupleAccessor;
    }

    @Override
    public void setConstrain(IPartitionedMemoryConstrain constrain) {
        this.constrain = constrain;
    }

    @Override
    public void reset() throws HyracksDataException {
        if (bufferManager != null) {
            for (int i = 0; i < bufferManager.getNumFrames(); i++) {
                framePool.deAllocateBuffer(bufferManager.getFrame(i, tempInfo).getBuffer());
            }
            bufferManager.reset();
        }
        numTuples = 0;
        appendFrame.reset(null);
    }


    @Override
    public int getNumTuples() {
        return numTuples;
    }

    @Override
    public int getPhysicalSize() {
        int size = 0;
        if (bufferManager != null) {
            for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
                size += bufferManager.getFrame(i, tempInfo).getLength();
            }
        }
        return size;
    }

    public void clearBuckets() throws HyracksDataException {
        if (bufferManager != null) {
            for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
                framePool.deAllocateBuffer(bufferManager.getFrame(i, tempInfo).getBuffer());
            }
            bufferManager.reset();
        }
        numTuples = 0;
    }
    public boolean insertTuple(byte[] byteArray, int[] fieldEndOffsets, int start, int size,
            TuplePointer pointer) throws HyracksDataException {
        int actualSize = calculateActualSize(fieldEndOffsets, size);
        int fid = getLastBufferOrCreateNewIfNotExist(actualSize);
        if (fid < 0) {
            return false;
        }
        bufferManager.getFrame(fid, tempInfo);
        int tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        if (tid < 0) {
            fid = createNewBuffer(actualSize);
            if (fid < 0) {
                return false;
            }
            bufferManager.getFrame(fid, tempInfo);
            tid = appendTupleToBuffer(tempInfo, fieldEndOffsets, byteArray, start, size);
        }
        pointer.reset(makeGroupFrameId(fid), tid);
        numTuples++;
        return true;
    }

    @Override
    public boolean insertTuple(IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer)
            throws HyracksDataException {
        return insertTuple(tupleAccessor.getBuffer().array(), null,
                tupleAccessor.getTupleStartOffset(tupleId), tupleAccessor.getTupleLength(tupleId), pointer);
    }


    public static int calculateActualSize(int[] fieldEndOffsets, int size) {
        if (fieldEndOffsets != null) {
            return FrameHelper.calcRequiredSpace(fieldEndOffsets.length, size);
        }
        return FrameHelper.calcRequiredSpace(0, size);
    }

    private int makeGroupFrameId(int fid) {
        return fid;
    }

    private int parsePartitionId(int externalFrameId) {
        return externalFrameId;
    }

    private int parseFrameIdInPartition(int externalFrameId) {
        return externalFrameId;
    }

    private int createNewBuffer(int size) throws HyracksDataException {
        ByteBuffer newBuffer = requestNewBufferFromPool(size);
        if (newBuffer == null) {
            return -1;
        }
        appendFrame.reset(newBuffer);
        appender.reset(appendFrame, true);
        return bufferManager.insertFrame(newBuffer);
    }

    private ByteBuffer requestNewBufferFromPool(int recordSize) throws HyracksDataException {
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(0, recordSize, framePool.getMinFrameSize());
        if ((double) frameSize / (double) framePool.getMinFrameSize() + getPhysicalSize() > Integer.MAX_VALUE) {
            return null;
        }
        return framePool.allocateFrame(frameSize);
    }

    private int appendTupleToBuffer(BufferInfo bufferInfo, int[] fieldEndOffsets, byte[] byteArray, int start, int size)
            throws HyracksDataException {
        assert bufferInfo.getStartOffset() == 0 : "Haven't supported yet in FrameTupleAppender";
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (fieldEndOffsets == null) {
            if (appender.append(byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        } else {
            if (appender.append(fieldEndOffsets, byteArray, start, size)) {
                return appender.getTupleCount() - 1;
            }
        }

        return -1;
    }

    private void deleteTupleFromBuffer(BufferInfo bufferInfo) throws HyracksDataException {
        if (bufferInfo.getBuffer() != appendFrame.getBuffer()) {
            appendFrame.reset(bufferInfo.getBuffer());
            appender.reset(appendFrame, false);
        }
        if (!appender.cancelAppend()) {
            throw new HyracksDataException("Undoing the last insertion in the given frame couldn't be done.");
        }
    }

    private int getLastBufferOrCreateNewIfNotExist(int actualSize) throws HyracksDataException {
        if (bufferManager == null || bufferManager.getNumFrames() == 0) {
            bufferManager = new FrameBufferManager();
            return createNewBuffer(actualSize);
        }
        return getLastBuffer();
    }

    private int getLastBuffer() throws HyracksDataException {
        return bufferManager.getNumFrames() - 1;
    }

    @Override
    public void close() {
            if (bufferManager != null) {
                bufferManager.close();
            }

        framePool.close();
        bufferManager = null;
    }

    @Override
    public ITuplePointerAccessor getTuplePointerAccessor(final RecordDescriptor recordDescriptor) {
        return new AbstractTuplePointerAccessor() {
            FrameTupleAccessor innerAccessor = new FrameTupleAccessor(recordDescriptor);

            @Override
            IFrameTupleAccessor getInnerAccessor() {
                return innerAccessor;
            }

            @Override
            void resetInnerAccessor(TuplePointer tuplePointer) {
                bufferManager
                        .getFrame(parseFrameIdInPartition(tuplePointer.getFrameIndex()), tempInfo);
                innerAccessor.reset(tempInfo.getBuffer(), tempInfo.getStartOffset(), tempInfo.getLength());
            }
        };
    }

    /*public void flushBucket(TuplePointer tuplePointer, IFrameWriter writer) throws HyracksDataException {
        int fIndex = tuplePointer.getFrameIndex();
        while(fIndex < bufferManager.getNumFrames()) {
            bufferManager.getFrame(tuplePointer.getFrameIndex(), tempInfo);
            tempInfo.getBuffer().position(tempInfo.getStartOffset());
            tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
            frameTupleAccessor.reset(tempInfo.getBuffer());
            writer.
            fIndex++;
        }
        if (bufferManager != null && getNumTuples() > 0) {
            bufferManager.getFrame(tuplePointer.getFrameIndex(), tempInfo);
            for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
                partition.getFrame(i, tempInfo);
                tempInfo.getBuffer().position(tempInfo.getStartOffset());
                tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
                writer.nextFrame(tempInfo.getBuffer());
            }
        }

    }*/

    public BufferInfo getFrame(int frameIndex, BufferInfo returnedInfo) {
        bufferManager.getFrame(frameIndex, returnedInfo);
        return returnedInfo;
    }

    @Override
    public IPartitionedMemoryConstrain getConstrain() {
        return constrain;
    }

}
