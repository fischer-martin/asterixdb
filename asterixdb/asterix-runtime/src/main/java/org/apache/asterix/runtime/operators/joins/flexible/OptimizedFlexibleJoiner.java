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

import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsSideTuple;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
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
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

public class OptimizedFlexibleJoiner {
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

    protected final FrameTupleAppender resultAppender;
    protected final FrameTupleCursor[] inputCursor;

    private final ITuplePairComparator tpComparator;
    private final IHyracksTaskContext ctx;

    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);

    private int counter = 0;

    private final int partition;

    //private final FlexibleJoin flexibleJoin;

    public OptimizedFlexibleJoiner(IHyracksTaskContext ctx, ITuplePairComparator tpComparator, int memorySize, int[] buildKeys,
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
        accessorOuter = new FrameTupleAccessor(probeRd);

        int outerBufferMngrMemBudgetInFrames = memorySize - 4;
        int outerBufferMngrMemBudgetInBytes = ctx.getInitialFrameSize() * outerBufferMngrMemBudgetInFrames;
        this.outerBufferMngr = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, outerBufferMngrMemBudgetInBytes), FrameFreeSlotPolicyFactory
                .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream(ctx, "fj-build");
        runFilePointer = new RunFilePointer();
        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));


        //flexibleJoin = null;
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputCursor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[BUILD_PARTITION].getAccessor().getTupleCount(); x++) {

            runFileStream.addToRunFile(inputCursor[BUILD_PARTITION].getAccessor(), x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
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
                    if(!this.bucketIdsMap.containsKey(currBuildBucketId+","+currProbeBucketId)) {
                        this.bucketIdsMap.put(
                                currBuildBucketId+","+currProbeBucketId ,
                                (tpComparator.compare(buildAccessor, currBuildTupleIdx, accessorOuter, currProbleTupleIdx) == 0)
                        );
                    }
                    matchResult = this.bucketIdsMap.get(currBuildBucketId+","+currProbeBucketId);

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

}
