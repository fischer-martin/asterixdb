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
import java.util.LinkedHashMap;

import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BucketBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class InMemoryThetaFlexibleJoiner {

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    protected final FrameTupleAppender resultAppender;
    protected final FrameTupleCursor[] inputCursor;

    private final IHyracksTaskContext ctx;

    private int memSizeInFrames;

    private ISimpleFrameBufferManager bufferManagerForHashTable;
    private BucketBufferManager bufferManager;
    private ITuplePointerAccessor memoryAccessor;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private final TuplePointer tempPtr = new TuplePointer();

    private ITuplePairComparator tpComparator;

    private final int nBuckets;

    private SerializableBucketIdList table;

    protected int numberOfBuckets = 0;


    protected boolean spilled;

    protected long numRecordsFromBuild;
//    private LinkedHashMap<Integer, Integer> bucketMap = new LinkedHashMap<>();
//        private LinkedHashMap<Integer, Integer> bucketMatchCount = new LinkedHashMap<>();
//        private LinkedHashMap<Integer, Long> spilledBucketMap = new LinkedHashMap<>();

    public InMemoryThetaFlexibleJoiner(IHyracksTaskContext ctx, int memorySize, RecordDescriptor buildRd,
            RecordDescriptor probeRd, int nBuckets) throws HyracksDataException {

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "FlexibleJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        this.ctx = ctx;
        this.nBuckets = nBuckets;

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        this.memSizeInFrames = memorySize;
        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);

        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(ctx, memSizeInFrames * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);

        table = new SerializableBucketIdList(nBuckets, ctx, bufferManagerForHashTable);

        this.bufferManager = new BucketBufferManager(framePool, buildRd);
        this.memoryAccessor = bufferManager.createTuplePointerAccessor();

        this.spilled = false;
        this.numRecordsFromBuild = 0;

    }

    public void buildOneBucket(ByteBuffer buffer, int bucketId, int startOffset, int endOffset)
            throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {

//                        if(accessorBuild.getTupleStartOffset(i) < startOffset) continue;
//                        if(endOffset != -1) {
//                            if(accessorBuild.getTupleStartOffset(i) >= endOffset) break;
//                        }
            // b = FlexibleJoinsUtil.getBucketId(accessorBuild,i,1);
            //int bucketIdT = FlexibleJoinsUtil.getBucketId(accessorBuild, i, 1);
            //bucketMap.merge(bucketIdT, 1, Integer::sum);
            //System.out.println("Start offset of "+ i +":"+accessorBuild.getTupleStartOffset(i));
            // If the memory does not accept the new record join should fail since buildOneBucket shall only be called for the buckets fit into memory
            if (!bufferManager.insertTuple(accessorBuild, i, tempPtr)) {
                throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY, "");
            }
            if (i == 0 && table.getBuildTuplePointer(bucketId) == null) {
                numberOfBuckets++;
                // If the table does not accept the new bucket id join should fail since buildOneBucket shall only be called for the buckets fit into memory
                if (!table.insert(bucketId, tempPtr, new TuplePointer())) {
                    throw HyracksDataException.create(ErrorCode.INSUFFICIENT_MEMORY, "");
                }
            }
            numRecordsFromBuild++;
        }
    }

    public void closeBuild() throws HyracksDataException {

    }

    public void initProbe(ITuplePairComparator comparator) {
//                StringBuilder a = new StringBuilder();
//                a.append("Bucket Counter From Build Side\n");
//                for(Integer bucketId: bucketMap.keySet()) {
//                    a.append(bucketId).append("\t").append(bucketMap.get(bucketId)).append("\n");
//                }
//                System.out.println(a);
        this.tpComparator = comparator;
    }

    private byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public void probeOneBucket(ByteBuffer buffer, IFrameWriter writer, int startOffset, int endOffset)
            throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        int numberOfBuckets = table.getNumEntries();
        int accessorIndex = 0;
        // for each record from S
        for (int i = 0; i < tupleCount; ++i) {
            boolean matched = false;
//                        if(accessorProbe.getTupleStartOffset(i) < startOffset) continue;
//                        if(endOffset != -1) {
//                            if(accessorProbe.getTupleStartOffset(i) >= endOffset) break;
//                        }
//            //int bucketIdT = FlexibleJoinsUtil.getBucketId(accessorProbe, i, 1);
            //bucketMatchCount.merge(bucketIdT, 1, Integer::sum);

            // Iterate over the buckets from bucket table
            for (int bucketIndex = 0; bucketIndex < numberOfBuckets; bucketIndex++) {
                int[] bucketInfo = table.getEntry(bucketIndex);
                memoryAccessor.reset(new TuplePointer(bucketInfo[1], bucketInfo[2]));
                accessorIndex = bucketInfo[2];
                // if buckets are matching
                //TODO Here we are not able to reach the data from memory if the bucket is already spilled
                if (this.tpComparator.compare(memoryAccessor, accessorIndex, accessorProbe, i) < 1) {
                    matched = true;
                    // if the bucket is in memory join the records
                    int tupleCounter = bucketInfo[2];
                    int frameCounter = bucketInfo[1];
                    boolean finished = false;
                    boolean first = true;

                    while (frameCounter < bufferManager.getNumberOfFrames()) {
                        if (!first) {
                            tupleCounter = 0;
                        }
                        while (tupleCounter < memoryAccessor.getTupleCount()) {
                            first = false;
                            memoryAccessor.reset(new TuplePointer(frameCounter, tupleCounter));
                            int bucketReadFromMem = FlexibleJoinsUtil.getBucketId(memoryAccessor, tupleCounter, 1);
                            if (bucketReadFromMem != bucketInfo[0]) {
                                finished = true;
                                break;
                            }
                            addToResult(memoryAccessor, tupleCounter, accessorProbe, i, writer);
                            tupleCounter++;

                        }
                        if (finished)
                            break;
                        frameCounter++;
                    }

                }
            }
            if(!matched) break;
        }

    }

    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
//                StringBuilder a = new StringBuilder();
//                a.append("Bucket Counter From Probe Side\n");
//                for(Integer bucketId: bucketMatchCount.keySet()) {
//                    a.append(bucketId).append("\t").append(bucketMatchCount.get(bucketId)).append("\n");
//                }
//                System.out.println(a);
        resultAppender.write(writer, true);
    }

    public void releaseResource() throws HyracksDataException {
        bufferManager.close();
        bufferManager = null;

        table.close();
        table = null;
    }

    private void addToResult(IFrameTupleAccessor buildAccessor, int buildTupleId, IFrameTupleAccessor probeAccessor,
            int probeTupleId, IFrameWriter writer) throws HyracksDataException {

        FrameUtils.appendConcatToWriter(writer, resultAppender, buildAccessor, buildTupleId, probeAccessor,
                probeTupleId);

    }

    public void printTableInfo() {
        table.printInfo();
    }

    public SerializableBucketIdList getBucketTable() {
        return table;
    }

}
