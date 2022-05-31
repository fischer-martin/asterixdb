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

//import org.apache.asterix.cartilage.base.FlexibleJoin;
import org.apache.asterix.om.base.ARectangle;
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
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OptimizedFlexibleJoinerSpatial {
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

    private final ITuplePairComparator tpComparator;
    private final IHyracksTaskContext ctx;

    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);

    private int counter = 0;

    private final int partition;

    private computeMBR computeMBROne = new computeMBR();
    private computeMBR computeMBRTwo = new computeMBR();
    private SpatialJoinConfiguration spatialJoinConfiguration;

    private HashMap<Integer, ArrayList<Rectangle>> buildRectangles = new HashMap<>();
    private HashMap<Integer, ArrayList<Rectangle>> probeRectangles = new HashMap<>();

    private HashMap<Integer, ArrayList<Integer>> buildBucketIds = new HashMap<>();
    private HashMap<Integer, ArrayList<Integer>> probeBucketIds = new HashMap<>();
    public OptimizedFlexibleJoinerSpatial(IHyracksTaskContext ctx, ITuplePairComparator tpComparator, int memorySize, int[] buildKeys,
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

        //Two frames are used for the runfile stream, and one frame for each input (2 outputs).
        //framePool = new DeallocatableFramePool(ctx, (memorySize - 4) * ctx.getInitialFrameSize());
        //bufferManager = new VariableDeletableTupleMemoryManager(framePool, probeRd);
        //memoryCursor = new TuplePointerCursor(bufferManager.createTuplePointerAccessor());


        int outerBufferMngrMemBudgetInFrames = memorySize - 4;
        int outerBufferMngrMemBudgetInBytes = ctx.getInitialFrameSize() * outerBufferMngrMemBudgetInFrames;
        this.outerBufferMngr = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, outerBufferMngrMemBudgetInBytes), FrameFreeSlotPolicyFactory
                .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream(ctx, "ofj-build");
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
            Rectangle aRectangle = FlexibleJoinsUtil.getRectangle(buildAccessor, currBuildTupleIdx, 0);
            if(buildBucketIds.containsKey(currBuildBucketId%20))
                buildBucketIds.get(currBuildBucketId%20).add(currBuildBucketId);
            else {
                ArrayList<Integer> newList = new ArrayList<>();
                newList.add(currBuildBucketId);
                buildBucketIds.put(currBuildBucketId%20, newList);
            }
            if(buildRectangles.containsKey(currBuildBucketId%20))
                buildRectangles.get(currBuildBucketId%20).add(aRectangle);
            else {
                ArrayList<Rectangle> newList = new ArrayList<>();
                newList.add(aRectangle);
                buildRectangles.put(currBuildBucketId%20, newList);
            }

            for (int i = 0; i < outerBufferFrameCount; i++) {
                BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                        outerBufferInfo.getLength());
                int probeTupleCount = accessorOuter.getTupleCount();
                for(int currProbleTupleIdx = 0; currProbleTupleIdx < probeTupleCount; currProbleTupleIdx++) {
                    currProbeBucketId = FlexibleJoinsUtil.getBucketId(accessorOuter, currProbleTupleIdx,1);
                    Rectangle bRectangle = FlexibleJoinsUtil.getRectangle(accessorOuter, currProbleTupleIdx, 0);

                    if(probeBucketIds.containsKey(currProbeBucketId%20))
                        probeBucketIds.get(currProbeBucketId%20).add(currProbeBucketId);
                    else {
                        ArrayList<Integer> newList = new ArrayList<>();
                        newList.add(currProbeBucketId);
                        probeBucketIds.put(currProbeBucketId%20, newList);
                    }
                    if(buildRectangles.containsKey(currProbeBucketId%20))
                        buildRectangles.get(currProbeBucketId%20).add(bRectangle);
                    else {
                        ArrayList<Rectangle> newList = new ArrayList<>();
                        newList.add(bRectangle);
                        buildRectangles.put(currProbeBucketId%20, newList);
                    }

                    computeMBRTwo.add(aRectangle);
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


        this.spatialJoinConfiguration = divide(this.computeMBROne, this.computeMBRTwo);

        for(Map.Entry<Integer, ArrayList<Integer>> partition : buildBucketIds.entrySet()) {
            Integer partitionId = partition.getKey();
            ArrayList<Integer> buildTileIds = partition.getValue();
            ArrayList<Integer> probeTileIds = probeBucketIds.get(partitionId);

            ArrayList<Rectangle> buildRectanglesa = buildRectangles.get(partitionId);
            ArrayList<Rectangle> probeRectanglesa = probeRectangles.get(partitionId);

            for(int i = 0; i < buildTileIds.size(); i++) {
                for(int j = 0; j < probeTileIds.size(); j++) {
                    Rectangle buildRectangle = buildRectanglesa.get(i);
                    Rectangle probeRectangle = probeRectanglesa.get(j);
                    int[] id1 = assign1(buildRectangle, this.spatialJoinConfiguration);
                    int[] id2 = assign1(probeRectangle, this.spatialJoinConfiguration);
                    int counter = 0;
                    for(int x :id1) {
                        for(int y: id2) {
                            counter++;
                        }
                    }
                    //System.out
                }
            }



        }
        /*for(int i = 0; i < buildBucketIds.size(); i++) {
            Rectangle buildRectangle = buildRectangles.get(i);
            assign1(buildRectangle, this.spatialJoinConfiguration);

        }

        for(int i = 0; i < probeBucketIds.size(); i++) {
            Rectangle probeRectangle = probeRectangles.get(i);
            assign1(probeRectangle, this.spatialJoinConfiguration);

        }

        for(int j = 0; j < probeBucketIds.size(); j++) {

            int probeBucketId = probeBucketIds.get(j);

        }*/


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

//    private boolean memoryHasTuples() {
//        return bufferManager.getNumTuples() > 0;
//    }

    public SpatialJoinConfiguration divide(computeMBR s1, computeMBR s2) {
        computeMBR c1 = (computeMBR) s1;
        computeMBR c2 = (computeMBR) s2;

        if (c1.MBR.x1 < c2.MBR.x1)
            c1.MBR.x1 = c2.MBR.x1;
        if (c1.MBR.x2 > c2.MBR.x2)
            c1.MBR.x2 = c2.MBR.x2;
        if (c1.MBR.y1 < c2.MBR.y1)
            c1.MBR.y1 = c2.MBR.y1;
        if (c1.MBR.y2 > c2.MBR.y2)
            c1.MBR.y2 = c2.MBR.y2;

        return new SpatialJoinConfiguration(c1.MBR, 10);
    }

    public int[] assign1(Rectangle k1, SpatialJoinConfiguration spatialJoinConfiguration) {
        ArrayList<Integer> tiles = new ArrayList<>();
        double minX = spatialJoinConfiguration.Grid.x1;
        double minY = spatialJoinConfiguration.Grid.y1;
        double maxX = spatialJoinConfiguration.Grid.x2;
        double maxY = spatialJoinConfiguration.Grid.y2;

        int rows = spatialJoinConfiguration.n;
        int columns = spatialJoinConfiguration.n;
        int row1 = (int) Math.ceil((k1.y1 - minY) * rows / (maxY - minY));
        int col1 = (int) Math.ceil((k1.x1 - minX) * columns / (maxX - minX));
        int row2 = (int) Math.ceil((k1.y2 - minY) * rows / (maxY - minY));
        int col2 = (int) Math.ceil((k1.x2 - minX) * columns / (maxX - minX));

        row1 = Math.min(Math.max(1, row1), rows * columns);
        col1 = Math.min(Math.max(1, col1), rows * columns);
        row2 = Math.min(Math.max(1, row2), rows * columns);
        col2 = Math.min(Math.max(1, col2), rows * columns);

        int minRow = Math.min(row1, row2);
        int maxRow = Math.max(row1, row2);
        int minCol = Math.min(col1, col2);
        int maxCol = Math.max(col1, col2);

        for (int i = minRow; i <= maxRow; i++) {
            for (int j = minCol; j <= maxCol; j++) {
                int tileId = (i - 1) * columns + j;
                tiles.add(tileId);
            }
        }

        return tiles.stream().mapToInt(i -> i).toArray();
    }

}

class SpatialJoinConfiguration {
    public Rectangle Grid;
    public int n;

    SpatialJoinConfiguration(Rectangle MBR, int n) {
        this.Grid = MBR;
        this.n = n;
    }
}

class computeMBR {
    public Rectangle MBR = new Rectangle();

    public void add(Rectangle k) {
        if (k.x1 < MBR.x1)
            MBR.x1 = k.x1;
        if (k.x2 > MBR.x2)
            MBR.x2 = k.x2;
        if (k.y1 < MBR.y1)
            MBR.y1 = k.y1;
        if (k.y2 > MBR.y2)
            MBR.y2 = k.y2;
    }

    public void add(computeMBR s) {
        computeMBR c = (computeMBR) s;
        if (c.MBR.x1 < MBR.x1)
            MBR.x1 = c.MBR.x1;
        if (c.MBR.x2 > MBR.x2)
            MBR.x2 = c.MBR.x2;
        if (c.MBR.y1 < MBR.y1)
            MBR.y1 = c.MBR.y1;
        if (c.MBR.y2 > MBR.y2)
            MBR.y2 = c.MBR.y2;
    }
}


