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
package org.apache.asterix.runtime.operators.joins.flexible;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsSideTuple;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.structures.KeyPair;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class FlexibleJoiner {
//    private final IDeallocatableFramePool framePool;
//    private final IDeletableTupleBufferManager bufferManager;
//    private final TuplePointerCursor memoryCursor;
    private final LinkedList<TuplePointer> memoryBuffer = new LinkedList<>();
    private final HashMap<String, Boolean> bucketIdsMap = new HashMap<>();

    private final VariableFrameMemoryManager outerBufferMngr;

    private final FrameTupleAccessor accessorOuter;

    private final RunFileStream runFileStream;
    private final RunFilePointer runFilePointer;

    private FlexibleJoinsSideTuple memoryTuple;
    private FlexibleJoinsSideTuple[] inputTuple;
    private TuplePointer tp;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final FrameTupleCursor[] inputCursor;

    private ISimpleFrameBufferManager bufferManagerForHashTable;

    private final ITuplePairComparator tpComparator;
    private final IHyracksTaskContext ctx;

    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);

    private int counter = 0;

    private final int partition;

    private SerializableBucketIdList table;
    private int buildTupleCounter = 0;

    public FlexibleJoiner(IHyracksTaskContext ctx, ITuplePairComparator tpComparator, int memorySize, int[] buildKeys,
            int[] probeKeys, RecordDescriptor buildRd, RecordDescriptor probeRd, int partition) throws HyracksDataException {

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "FlexibleJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        this.ctx = ctx;
        this.tpComparator = tpComparator;
        this.partition = partition;

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        inputCursor[PROBE_PARTITION] = new FrameTupleCursor(probeRd);
        accessorOuter = new FrameTupleAccessor(probeRd);


        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);

        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(ctx, memorySize * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);


        int outerBufferMngrMemBudgetInFrames = memorySize - 4;
        int outerBufferMngrMemBudgetInBytes = ctx.getInitialFrameSize() * outerBufferMngrMemBudgetInFrames;
        this.outerBufferMngr = new VariableFrameMemoryManager(
                framePool, FrameFreeSlotPolicyFactory
                .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream(ctx, "fj-build");
        runFilePointer = new RunFilePointer();
        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();

        //memoryTuple = new FlexibleJoinsSideTuple(tpComparator, memoryCursor, probeKeys);

        inputTuple = new FlexibleJoinsSideTuple[JOIN_PARTITIONS];
        inputTuple[PROBE_PARTITION] = new FlexibleJoinsSideTuple(tpComparator, inputCursor[PROBE_PARTITION], probeKeys);
        inputTuple[BUILD_PARTITION] = new FlexibleJoinsSideTuple(tpComparator, inputCursor[BUILD_PARTITION], buildKeys);

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }


    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputCursor[BUILD_PARTITION].reset(buffer);
        buildTupleCounter += inputCursor[BUILD_PARTITION].getAccessor().getTupleCount();
        for (int x = 0; x < inputCursor[BUILD_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream.addToRunFile(inputCursor[BUILD_PARTITION].getAccessor(), x);
        }
    }

    public void processBuildClose() throws HyracksDataException {

        table = new SerializableBucketIdList(buildTupleCounter, ctx, bufferManagerForHashTable);

        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(inputCursor[BUILD_PARTITION]);

    }

    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorOuter.reset(buffer);
        if (accessorOuter.getTupleCount() <= 0) {
            return;
        }
        if (outerBufferMngr.insertFrame(buffer) < 0) {
            join(writer);
            outerBufferMngr.reset();
            if (outerBufferMngr.insertFrame(buffer) < 0) {
                throw new HyracksDataException("The given outer frame of size:" + buffer.capacity()
                        + " is too big to cache in the buffer. Please choose a larger buffer memory size");
            }
        }
    }

    public void join(IFrameWriter writer) throws HyracksDataException {
        int outerBufferFrameCount = outerBufferMngr.getNumFrames();
        if (outerBufferFrameCount == 0) {
            return;
        }
        int currBuildBucketId = 0;
        int currProbeBucketId = 0;

        int lastBuildBucketId = -1;
        int lastProbeBucketId = -1;

        boolean first = true;
        boolean addToResults = false;
        boolean matchResult = false;
        IFrameTupleAccessor buildAccessor = inputCursor[BUILD_PARTITION].getAccessor();
        int currBuildTupleIdx = 0;
        while (buildHasNext()) {
            currBuildTupleIdx = inputCursor[BUILD_PARTITION].getTupleId() + 1;
            currBuildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor,currBuildTupleIdx,1);
            for (int i = 0; i < outerBufferFrameCount; i++) {
                BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                        outerBufferInfo.getLength());
                int probeTupleCount = accessorOuter.getTupleCount();
                for(int currProbleTupleIdx = 0; currProbleTupleIdx < probeTupleCount; currProbleTupleIdx++) {
                    currProbeBucketId = FlexibleJoinsUtil.getBucketId(accessorOuter, currProbleTupleIdx,1);
                    //System.out.println("build: " + currBuildBucketId + "\tprobe:"+currProbeBucketId);
                counter++;
                if(lastProbeBucketId != currProbeBucketId || lastBuildBucketId != currBuildBucketId) {
                    /*if(!this.table.getResult(new KeyPair(currBuildBucketId, currProbeBucketId))) {
                        this.table.insert(new KeyPair(currBuildBucketId, currProbeBucketId),
                                (tpComparator.compare(buildAccessor, currBuildTupleIdx, accessorOuter, currProbleTupleIdx) == 0)
                        );
                    }
                    matchResult = this.table.getResult(new KeyPair(currBuildBucketId, currProbeBucketId));*/

                    lastProbeBucketId = currProbeBucketId;
                    lastBuildBucketId = currBuildBucketId;
                }
                    //matchResult = (tpComparator.compare(buildAccessor, currBuildTupleIdx, accessorOuter, currProbleTupleIdx) == 0);
                    if(matchResult) {
                        addToResult(buildAccessor, currBuildTupleIdx, accessorOuter, currProbleTupleIdx, writer);
                    }
                }
            }

            inputCursor[BUILD_PARTITION].next();
        }
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {
        join(writer);

        resultAppender.write(writer, true);
        runFileStream.close();
        runFileStream.removeRunFile();
        System.out.println("join counter"+counter);

    }

    private boolean buildHasNext() throws HyracksDataException {
        if (!inputCursor[BUILD_PARTITION].hasNext()) {
            // Must keep condition in a separate `if` due to actions applied in loadNextBuffer.
            return runFileStream.loadNextBuffer(inputCursor[BUILD_PARTITION]);
        } else {
            return true;
        }
    }
//
//    private void processBuildTuple(IFrameWriter writer) throws HyracksDataException {
//        // Check against memory
//        if (memoryHasTuples()) {
//            memoryCursor.reset(memoryBuffer.iterator());
//            while (memoryCursor.hasNext()) {
//                memoryCursor.next();
//                if (inputTuple[BUILD_PARTITION].removeFromMemory(memoryTuple)) {
//                    // remove from memory
//                    bufferManager.deleteTuple(memoryCursor.getTuplePointer());
//                    memoryCursor.remove();
//                    continue;
//                } else if (inputTuple[BUILD_PARTITION].checkForEarlyExit(memoryTuple)) {
//                    // No more possible comparisons
//                    break;
//                } else if (inputTuple[BUILD_PARTITION].compareJoin(memoryTuple)) {
//                    // add to result
//                    addToResult(inputCursor[BUILD_PARTITION].getAccessor(), inputCursor[BUILD_PARTITION].getTupleId(),
//                            memoryCursor.getAccessor(), memoryCursor.getTupleId(), writer);
//                }
//            }
//        }
//    }

//    private void processProbeTuple(IFrameWriter writer) throws HyracksDataException {
//        // append to memory
//        // BUILD Cursor is guaranteed to have next
//        if (tpComparator.compare(inputCursor[BUILD_PARTITION].getAccessor(),
//                inputCursor[BUILD_PARTITION].getTupleId() + 1, inputCursor[PROBE_PARTITION].getAccessor(),
//                inputCursor[PROBE_PARTITION].getTupleId()) == 0) {
//            if (!addToMemory(inputCursor[PROBE_PARTITION].getAccessor(), inputCursor[PROBE_PARTITION].getTupleId())) {
//                unfreezeAndClearMemory(writer);
//                if (!addToMemory(inputCursor[PROBE_PARTITION].getAccessor(),
//                        inputCursor[PROBE_PARTITION].getTupleId())) {
//                    throw new RuntimeException("Should Never get called.");
//                }
//            }
//        }
//    }

//    private void unfreezeAndClearMemory(IFrameWriter writer) throws HyracksDataException {
//        runFilePointer.reset(runFileStream.getReadPointer(), inputCursor[BUILD_PARTITION].getTupleId());
//        while (buildHasNext() && memoryHasTuples()) {
//            // Process build side from runfile
//            inputCursor[BUILD_PARTITION].next();
//            processBuildTuple(writer);
//        }
//        // Clear memory
//        memoryBuffer.clear();
//        bufferManager.reset();
//        // Start reading
//        runFileStream.startReadingRunFile(inputCursor[BUILD_PARTITION], runFilePointer.getFileOffset());
//        inputCursor[BUILD_PARTITION].resetPosition(runFilePointer.getTupleIndex());
//    }

//    private boolean addToMemory(IFrameTupleAccessor accessor, int tupleId) throws HyracksDataException {
//        tp = new TuplePointer();
//        if (bufferManager.insertTuple(accessor, tupleId, tp)) {
//            memoryBuffer.add(tp);
//            return true;
//        }
//        return false;
//    }

    private void addToResult(IFrameTupleAccessor buildAccessor, int buildTupleId, IFrameTupleAccessor probeAccessor,
            int probeTupleId, IFrameWriter writer) throws HyracksDataException {
        //System.out.println("build tuple id: " + buildTupleId + "\tprobe tuple id:"+probeTupleId);
        FrameUtils.appendConcatToWriter(writer, resultAppender, buildAccessor, buildTupleId, probeAccessor,
                probeTupleId);
    }

    public void closeCache() throws HyracksDataException {
        if (runFileStream != null) {
            runFileStream.close();
        }
    }

    public void releaseMemory() throws HyracksDataException {
        outerBufferMngr.reset();
    }

//    private boolean memoryHasTuples() {
//        return bufferManager.getNumTuples() > 0;
//    }

}
